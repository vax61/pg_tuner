package schema

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PreloadConfig holds configuration for massive data preloading.
type PreloadConfig struct {
	// TargetSize is the target data size in bytes (e.g., 10GB = 10 * 1024^3)
	TargetSize int64
	// Parallel is the number of parallel goroutines for data generation
	Parallel int
	// Seed is the random seed for reproducible data generation
	Seed int64
	// ProgressCallback is called with progress updates (can be nil)
	ProgressCallback func(table string, loaded, total int64)
}

// PreloadStats holds statistics from a preload operation.
type PreloadStats struct {
	Customers    int64
	Branches     int64
	AccountTypes int64
	Accounts     int64
	Transactions int64
	Duration     time.Duration
	BytesLoaded  int64
}

// PreloadManager handles massive data loading using PostgreSQL COPY.
type PreloadManager struct {
	pool   *pgxpool.Pool
	cfg    PreloadConfig
	rng    *rand.Rand
	mu     sync.Mutex
	stats  PreloadStats
}

// NewPreloadManager creates a new preload manager.
func NewPreloadManager(pool *pgxpool.Pool, cfg PreloadConfig) *PreloadManager {
	if cfg.Parallel < 1 {
		cfg.Parallel = 4
	}
	return &PreloadManager{
		pool: pool,
		cfg:  cfg,
		rng:  rand.New(rand.NewSource(cfg.Seed)),
	}
}

// EstimateRowCounts estimates the number of rows needed to achieve the target size.
// Returns: customers, branches, accounts, transactions
func (pm *PreloadManager) EstimateRowCounts() (customers, branches, accounts, transactions int64) {
	// Approximate row sizes in bytes (including indexes)
	const (
		customerRowSize    = 350  // ~350 bytes per customer row with indexes
		branchRowSize      = 300  // ~300 bytes per branch row with indexes
		accountRowSize     = 250  // ~250 bytes per account row with indexes
		transactionRowSize = 150  // ~150 bytes per transaction row with indexes
	)

	// Transactions dominate the data size (~66% of total)
	// Use ratios: 1 customer : 2 accounts : 20 transactions
	// 1 branch per 200 accounts

	// Calculate based on transaction-dominated sizing
	// transactions = ~66% of target size
	transactionBytes := int64(float64(pm.cfg.TargetSize) * 0.66)
	transactions = transactionBytes / transactionRowSize

	// accounts = transactions / 10 (10 tx per account)
	accounts = transactions / 10
	if accounts < 10000 {
		accounts = 10000
	}

	// customers = accounts / 2 (2 accounts per customer)
	customers = accounts / 2
	if customers < 5000 {
		customers = 5000
	}

	// branches = accounts / 200
	branches = accounts / 200
	if branches < 50 {
		branches = 50
	}

	return customers, branches, accounts, transactions
}

// Preload performs massive data loading to achieve the target size.
func (pm *PreloadManager) Preload(ctx context.Context) error {
	start := time.Now()

	customers, branches, accounts, transactions := pm.EstimateRowCounts()

	pm.progress("Estimated rows: customers=%d, branches=%d, accounts=%d, transactions=%d",
		customers, branches, accounts, transactions)

	// Load account types (static, small)
	if err := seedAccountTypes(ctx, pm.pool); err != nil {
		return fmt.Errorf("loading account types: %w", err)
	}
	pm.stats.AccountTypes = accountTypesStatic

	// Load branches using COPY
	if err := pm.loadBranches(ctx, branches); err != nil {
		return fmt.Errorf("loading branches: %w", err)
	}

	// Load customers using COPY
	if err := pm.loadCustomers(ctx, customers); err != nil {
		return fmt.Errorf("loading customers: %w", err)
	}

	// Load accounts using COPY
	if err := pm.loadAccounts(ctx, accounts, customers, branches); err != nil {
		return fmt.Errorf("loading accounts: %w", err)
	}

	// Load transactions using COPY with parallelism
	if err := pm.loadTransactionsParallel(ctx, transactions, accounts); err != nil {
		return fmt.Errorf("loading transactions: %w", err)
	}

	// Analyze tables
	pm.progress("Analyzing tables...")
	if _, err := pm.pool.Exec(ctx, "ANALYZE customers, branches, account_types, accounts, transactions"); err != nil {
		return fmt.Errorf("analyzing tables: %w", err)
	}

	pm.stats.Duration = time.Since(start)

	return nil
}

// Stats returns the preload statistics.
func (pm *PreloadManager) Stats() PreloadStats {
	return pm.stats
}

func (pm *PreloadManager) loadBranches(ctx context.Context, count int64) error {
	pm.progress("Loading %d branches using COPY...", count)

	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- pm.generateBranchesCSV(pw, count)
	}()

	conn, err := pm.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	copySQL := `COPY branches (branch_code, name, address, city, state, zip_code, phone, manager_name, opened_at, is_active) FROM STDIN WITH (FORMAT csv)`

	_, err = conn.Conn().PgConn().CopyFrom(ctx, pr, copySQL)
	if err != nil {
		return fmt.Errorf("COPY branches: %w", err)
	}

	if genErr := <-errCh; genErr != nil {
		return fmt.Errorf("generating branches data: %w", genErr)
	}

	pm.stats.Branches = count
	pm.reportProgress("branches", count, count)
	return nil
}

func (pm *PreloadManager) generateBranchesCSV(w io.Writer, count int64) error {
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}

	rng := rand.New(rand.NewSource(pm.cfg.Seed))

	for i := int64(1); i <= count; i++ {
		branchCode := fmt.Sprintf("BR%06d", i)
		name := fmt.Sprintf("Branch %d", i)
		cityIdx := rng.Intn(len(cities))
		address := fmt.Sprintf("%d Main St", rng.Intn(9999)+1)
		zipCode := fmt.Sprintf("%05d", rng.Intn(99999))
		phone := fmt.Sprintf("555-%03d-%04d", rng.Intn(999), rng.Intn(9999))
		managerName := fmt.Sprintf("Manager %d", i)
		openedAt := time.Now().AddDate(-rng.Intn(20), 0, 0).Format("2006-01-02")

		line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,true\n",
			branchCode, name, address, cities[cityIdx], states[cityIdx], zipCode, phone, managerName, openedAt)

		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}
	}

	return nil
}

func (pm *PreloadManager) loadCustomers(ctx context.Context, count int64) error {
	pm.progress("Loading %d customers using COPY...", count)

	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- pm.generateCustomersCSV(pw, count)
	}()

	conn, err := pm.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	copySQL := `COPY customers (customer_number, first_name, last_name, email, phone, address, city, state, zip_code, customer_type) FROM STDIN WITH (FORMAT csv)`

	_, err = conn.Conn().PgConn().CopyFrom(ctx, pr, copySQL)
	if err != nil {
		return fmt.Errorf("COPY customers: %w", err)
	}

	if genErr := <-errCh; genErr != nil {
		return fmt.Errorf("generating customers data: %w", genErr)
	}

	pm.stats.Customers = count
	pm.reportProgress("customers", count, count)
	return nil
}

func (pm *PreloadManager) generateCustomersCSV(w io.Writer, count int64) error {
	firstNames := []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	customerTypes := []string{"individual", "business"}
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}

	rng := rand.New(rand.NewSource(pm.cfg.Seed + 1))

	for i := int64(1); i <= count; i++ {
		customerNum := fmt.Sprintf("CUS%012d", i)
		firstName := firstNames[rng.Intn(len(firstNames))]
		lastName := lastNames[rng.Intn(len(lastNames))]
		email := fmt.Sprintf("%s.%s%d@example.com", firstName, lastName, i)
		phone := fmt.Sprintf("555-%03d-%04d", rng.Intn(999), rng.Intn(9999))
		address := fmt.Sprintf("%d Oak St", rng.Intn(9999)+1)
		cityIdx := rng.Intn(len(cities))
		zipCode := fmt.Sprintf("%05d", rng.Intn(99999))
		custType := customerTypes[rng.Intn(len(customerTypes))]

		line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			customerNum, firstName, lastName, email, phone, address, cities[cityIdx], states[cityIdx], zipCode, custType)

		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}

		if i%100000 == 0 {
			pm.reportProgress("customers", i, count)
		}
	}

	return nil
}

func (pm *PreloadManager) loadAccounts(ctx context.Context, count, numCustomers, numBranches int64) error {
	pm.progress("Loading %d accounts using COPY...", count)

	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- pm.generateAccountsCSV(pw, count, numCustomers, numBranches)
	}()

	conn, err := pm.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	copySQL := `COPY accounts (account_number, customer_id, branch_id, account_type_id, name, email, balance, status) FROM STDIN WITH (FORMAT csv)`

	_, err = conn.Conn().PgConn().CopyFrom(ctx, pr, copySQL)
	if err != nil {
		return fmt.Errorf("COPY accounts: %w", err)
	}

	if genErr := <-errCh; genErr != nil {
		return fmt.Errorf("generating accounts data: %w", genErr)
	}

	pm.stats.Accounts = count
	pm.reportProgress("accounts", count, count)
	return nil
}

func (pm *PreloadManager) generateAccountsCSV(w io.Writer, count, numCustomers, numBranches int64) error {
	statuses := []string{"active", "inactive", "suspended", "pending"}
	numAccountTypes := int64(accountTypesStatic)

	rng := rand.New(rand.NewSource(pm.cfg.Seed + 2))

	for i := int64(1); i <= count; i++ {
		accountNum := fmt.Sprintf("ACC%012d", i)
		customerID := rng.Int63n(numCustomers) + 1
		branchID := rng.Int63n(numBranches) + 1
		accountTypeID := rng.Int63n(numAccountTypes) + 1
		name := fmt.Sprintf("Account %d", i)
		email := fmt.Sprintf("account%d@example.com", i)
		balance := float64(rng.Intn(100000)) + rng.Float64()
		status := statuses[rng.Intn(len(statuses))]

		line := fmt.Sprintf("%s,%d,%d,%d,%s,%s,%.2f,%s\n",
			accountNum, customerID, branchID, accountTypeID, name, email, balance, status)

		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}

		if i%100000 == 0 {
			pm.reportProgress("accounts", i, count)
		}
	}

	return nil
}

func (pm *PreloadManager) loadTransactionsParallel(ctx context.Context, count, numAccounts int64) error {
	pm.progress("Loading %d transactions using COPY with %d parallel workers...", count, pm.cfg.Parallel)

	// Split work among parallel workers
	perWorker := count / int64(pm.cfg.Parallel)
	remainder := count % int64(pm.cfg.Parallel)

	var wg sync.WaitGroup
	errCh := make(chan error, pm.cfg.Parallel)
	var totalLoaded atomic.Int64

	for w := 0; w < pm.cfg.Parallel; w++ {
		workerCount := perWorker
		if w == 0 {
			workerCount += remainder
		}
		startID := int64(w)*perWorker + 1
		if w > 0 {
			startID += remainder
		}

		wg.Add(1)
		go func(workerID int, startID, workerCount int64) {
			defer wg.Done()
			err := pm.loadTransactionsWorker(ctx, workerID, startID, workerCount, numAccounts, &totalLoaded, count)
			if err != nil {
				errCh <- fmt.Errorf("worker %d: %w", workerID, err)
			}
		}(w, startID, workerCount)
	}

	wg.Wait()
	close(errCh)

	// Collect any errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("parallel load failed: %v", errs[0])
	}

	pm.stats.Transactions = totalLoaded.Load()
	pm.reportProgress("transactions", pm.stats.Transactions, count)
	return nil
}

func (pm *PreloadManager) loadTransactionsWorker(ctx context.Context, workerID int, startID, count, numAccounts int64, totalLoaded *atomic.Int64, totalCount int64) error {
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		errCh <- pm.generateTransactionsCSV(pw, workerID, startID, count, numAccounts, totalLoaded, totalCount)
	}()

	conn, err := pm.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	copySQL := `COPY transactions (account_id, type, amount, description) FROM STDIN WITH (FORMAT csv)`

	_, err = conn.Conn().PgConn().CopyFrom(ctx, pr, copySQL)
	if err != nil {
		return fmt.Errorf("COPY transactions: %w", err)
	}

	if genErr := <-errCh; genErr != nil {
		return fmt.Errorf("generating transactions data: %w", genErr)
	}

	return nil
}

func (pm *PreloadManager) generateTransactionsCSV(w io.Writer, workerID int, startID, count, numAccounts int64, totalLoaded *atomic.Int64, totalCount int64) error {
	txTypes := []string{"deposit", "withdrawal", "transfer", "fee", "interest"}

	rng := rand.New(rand.NewSource(pm.cfg.Seed + int64(workerID) + 100))

	for i := int64(0); i < count; i++ {
		accountID := rng.Int63n(numAccounts) + 1
		txType := txTypes[rng.Intn(len(txTypes))]
		amount := float64(rng.Intn(10000)) + rng.Float64()
		if txType == "withdrawal" || txType == "fee" {
			amount = -amount
		}
		desc := fmt.Sprintf("%s transaction #%d", txType, startID+i)

		// Escape description for CSV (replace commas and quotes)
		line := fmt.Sprintf("%d,%s,%.2f,\"%s\"\n", accountID, txType, amount, desc)

		if _, err := w.Write([]byte(line)); err != nil {
			return err
		}

		if i%100000 == 0 {
			loaded := totalLoaded.Add(100000)
			pm.reportProgress("transactions", loaded, totalCount)
		}
	}

	// Add remaining count
	remaining := count % 100000
	if remaining > 0 {
		totalLoaded.Add(remaining)
	}

	return nil
}

func (pm *PreloadManager) progress(format string, args ...interface{}) {
	if pm.cfg.ProgressCallback != nil {
		pm.cfg.ProgressCallback("info", 0, 0)
	}
	fmt.Printf(format+"\n", args...)
}

func (pm *PreloadManager) reportProgress(table string, loaded, total int64) {
	if pm.cfg.ProgressCallback != nil {
		pm.cfg.ProgressCallback(table, loaded, total)
	}
}

// GetTableSizes returns the size of each table in bytes.
func GetTableSizes(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	query := `
		SELECT
			relname as table_name,
			pg_total_relation_size(relid) as total_size
		FROM pg_catalog.pg_statio_user_tables
		WHERE schemaname = 'public'
		ORDER BY pg_total_relation_size(relid) DESC
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying table sizes: %w", err)
	}
	defer rows.Close()

	sizes := make(map[string]int64)
	for rows.Next() {
		var tableName string
		var size int64
		if err := rows.Scan(&tableName, &size); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		sizes[tableName] = size
	}

	return sizes, nil
}

// GetTotalDataSize returns the total size of all tables in bytes.
func GetTotalDataSize(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	sizes, err := GetTableSizes(ctx, pool)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, size := range sizes {
		total += size
	}
	return total, nil
}

// FormatBytes formats bytes as human-readable string.
func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// ParseSizeBytes parses a human-readable size string into bytes.
func ParseSizeBytes(s string) (int64, error) {
	var value float64
	var unit string

	_, err := fmt.Sscanf(s, "%f%s", &value, &unit)
	if err != nil {
		// Try without space
		for i := len(s) - 1; i >= 0; i-- {
			if s[i] >= '0' && s[i] <= '9' || s[i] == '.' {
				unit = s[i+1:]
				value = 0
				fmt.Sscanf(s[:i+1], "%f", &value)
				break
			}
		}
	}

	multiplier := int64(1)
	switch unit {
	case "TB", "tb", "T", "t":
		multiplier = 1 << 40
	case "GB", "gb", "G", "g":
		multiplier = 1 << 30
	case "MB", "mb", "M", "m":
		multiplier = 1 << 20
	case "KB", "kb", "K", "k":
		multiplier = 1 << 10
	case "B", "b", "":
		multiplier = 1
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}

	return int64(value * float64(multiplier)), nil
}
