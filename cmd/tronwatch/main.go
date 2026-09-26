package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yooud/tronwatch/internal/app"
	appconfig "github.com/yooud/tronwatch/internal/config"
	"github.com/yooud/tronwatch/internal/filter"
	"github.com/yooud/tronwatch/internal/ingest"
	"github.com/yooud/tronwatch/internal/model"
	"github.com/yooud/tronwatch/internal/p2p"
	"github.com/yooud/tronwatch/internal/store"
	appversion "github.com/yooud/tronwatch/internal/version"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "check-config" {
		if err := runCheckConfig(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "tronwatch check-config: %v\n", err)
			return 1
		}
		return 0
	}
	if len(args) > 0 && args[0] == "list" {
		if err := runList(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "tronwatch list: %v\n", err)
			return 1
		}
		return 0
	}
	if len(args) > 0 && args[0] == "check-db" {
		if err := runCheckDB(args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "tronwatch check-db: %v\n", err)
			return 1
		}
		return 0
	}
	if len(args) > 0 && args[0] == "probe" {
		if err := runProbe(ctx, args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "tronwatch probe: %v\n", err)
			return 1
		}
		return 0
	}
	if len(args) > 0 && args[0] == "version" {
		fmt.Fprintln(stdout, appversion.Value)
		return 0
	}
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	if err := runDaemon(ctx, args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "tronwatch: %v\n", err)
		return 1
	}
	return 0
}

func runCheckDB(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("check-db", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("db", "data/transactions.db", "BoltDB path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional argument %q", flags.Arg(0))
	}
	database, err := store.Open(*databasePath)
	if err != nil {
		return err
	}
	checkErr := database.Check()
	closeErr := database.Close()
	if err := errors.Join(checkErr, closeErr); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "database valid")
	return nil
}

func runProbe(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("probe", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("url", "http://127.0.0.1:9464/readyz", "readiness URL")
	timeout := flags.Duration("timeout", 3*time.Second, "probe timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 {
		return errors.New("probe requires no positional arguments and a positive timeout")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, *endpoint, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: *timeout}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness returned HTTP %d", response.StatusCode)
	}
	fmt.Fprintln(stdout, "ready")
	return nil
}

func runDaemon(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "JSON project configuration path")
	var peers stringListFlag
	flags.Var(&peers, "peer", "TRON peer host:port (repeat for multiple peers)")
	networkID := flags.Int("network-id", 201910292, "TRON P2P network ID")
	watchlistPath := flags.String("watchlist", "watchlist.json", "JSON watchlist path")
	databasePath := flags.String("db", "data/transactions.db", "BoltDB path")
	advertiseIP := flags.String("advertise-ip", "127.0.0.1", "IPv4 address sent in peer hello")
	reloadInterval := flags.Duration("reload-interval", time.Second, "watchlist poll interval")
	jsonLog := flags.Bool("json-log", false, "write structured JSON logs")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional argument %q", flags.Arg(0))
	}
	if *configPath != "" {
		var incompatible string
		flags.Visit(func(flagValue *flag.Flag) {
			if flagValue.Name != "config" && incompatible == "" {
				incompatible = flagValue.Name
			}
		})
		if incompatible != "" {
			return fmt.Errorf("--config cannot be combined with --%s", incompatible)
		}
		configuration, err := appconfig.Load(*configPath)
		if err != nil {
			return err
		}
		logger := newLogger(stderr, configuration.Logging.JSON)
		application, err := app.New(configuration, stdout, logger)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := application.Close(); closeErr != nil {
				logger.Error("closing application", "error", closeErr)
			}
		}()
		return application.Run(ctx)
	}
	if *networkID <= 0 || int64(*networkID) > int64(^uint32(0)>>1) {
		return fmt.Errorf("network-id %d is outside int32 range", *networkID)
	}
	if *reloadInterval <= 0 {
		return errors.New("reload-interval must be positive")
	}
	if len(peers) == 0 {
		peers = append(peers, "127.0.0.1:18888")
	}
	logger := newLogger(stderr, *jsonLog)
	watches, err := filter.OpenWatchlist(*watchlistPath)
	if err != nil {
		return err
	}
	database, err := store.Open(*databasePath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := database.Close(); closeErr != nil {
			logger.Error("closing database", "error", closeErr)
		}
	}()
	addresses, contracts := watches.Current().Sizes()
	logger.Info("watchlist loaded", "addresses", addresses, "contracts", contracts)
	loggingStore := &logStore{next: database, logger: logger}
	ingestor := ingest.New(watches, loggingStore, time.Now)
	peerPool, err := p2p.NewPool(peers, p2p.Config{
		NetworkID:   int32(*networkID),
		AdvertiseIP: *advertiseIP,
		Logger:      logger,
	}, ingestor)
	if err != nil {
		return err
	}

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		return peerPool.Run(groupContext)
	})
	group.Go(func() error {
		return reloadWatchlist(groupContext, watches, *reloadInterval, logger)
	})
	group.Go(func() error {
		return reportStats(groupContext, ingestor, logger)
	})
	return group.Wait()
}

type stringListFlag []string

func (values *stringListFlag) String() string {
	return fmt.Sprint([]string(*values))
}

func (values *stringListFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func runCheckConfig(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("check-config", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "config.json", "JSON project configuration path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional argument %q", flags.Arg(0))
	}
	if _, err := appconfig.Load(*configPath); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "configuration valid")
	return nil
}

func reloadWatchlist(
	ctx context.Context,
	watches *filter.Watchlist,
	interval time.Duration,
	logger *slog.Logger,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			changed, err := watches.Refresh()
			if err != nil {
				logger.Warn("watchlist update rejected; keeping last good snapshot", "error", err)
				continue
			}
			if changed {
				addresses, contracts := watches.Current().Sizes()
				logger.Info("watchlist reloaded", "addresses", addresses, "contracts", contracts)
			}
		}
	}
}

func reportStats(ctx context.Context, ingestor *ingest.Ingestor, logger *slog.Logger) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			stats := ingestor.Stats()
			logger.Info("transaction totals", "seen", stats.Seen, "matched", stats.Matched)
		}
	}
}

func runList(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("db", "data/transactions.db", "BoltDB path")
	limit := flags.Int("limit", 100, "maximum records")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional argument %q", flags.Arg(0))
	}
	if *limit <= 0 {
		return errors.New("limit must be positive")
	}
	database, err := store.Open(*databasePath)
	if err != nil {
		return err
	}
	records, err := database.List(*limit)
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
	if err := database.Close(); err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("encoding record: %w", err)
		}
	}
	return nil
}

func newLogger(output io.Writer, jsonOutput bool) *slog.Logger {
	options := &slog.HandlerOptions{Level: slog.LevelInfo}
	if jsonOutput {
		return slog.New(slog.NewJSONHandler(output, options))
	}
	return slog.New(slog.NewTextHandler(output, options))
}

type logStore struct {
	next   *store.DB
	logger *slog.Logger
}

func (s *logStore) Put(record model.Record) error {
	changed, err := s.next.PutIfChanged(record)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	s.logger.Info(
		"matched transaction stored",
		"tx_id", record.TxID,
		"source", record.Source,
		"block", optionalBlockNumber(record.BlockNumber),
		"matches", len(record.Matches),
	)
	return nil
}

func (s *logStore) Exists(txID string) (bool, error) {
	return s.next.Exists(txID)
}

func (s *logStore) ApplyBlock(block model.Block, records []model.Record) (model.ChainUpdate, error) {
	update, err := s.next.ApplyBlock(block, records)
	if err != nil {
		return update, err
	}
	if update.Reorg {
		s.logger.Warn("canonical chain reorganized", "old_tip", update.OldTipID, "new_tip", update.NewTipID, "depth", update.ReorgDepth)
	}
	return update, nil
}

func optionalBlockNumber(blockNumber *int64) any {
	if blockNumber == nil {
		return nil
	}
	return *blockNumber
}
