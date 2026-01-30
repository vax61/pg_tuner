package schema

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Scale factor multipliers (per unit of scale)
	customersPerScale    = 5_000
	branchesPerScale     = 50
	accountTypesStatic   = 5
	accountsPerScale     = 10_000
	transactionsPerScale = 100_000
)

// TableStats holds counts for all tables.
type TableStats struct {
	Customers    int64
	Branches     int64
	AccountTypes int64
	Accounts     int64
	Transactions int64
	AuditLog     int64
}

// CreateOLTPSchema creates the OLTP benchmark tables with full relational model.
func CreateOLTPSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := `
		-- Customers table: main customer entity
		CREATE TABLE IF NOT EXISTS customers (
			id BIGSERIAL PRIMARY KEY,
			customer_number VARCHAR(20) NOT NULL UNIQUE,
			first_name VARCHAR(50) NOT NULL,
			last_name VARCHAR(50) NOT NULL,
			email VARCHAR(100),
			phone VARCHAR(20),
			address VARCHAR(200),
			city VARCHAR(50),
			state VARCHAR(50),
			zip_code VARCHAR(20),
			customer_type VARCHAR(20) NOT NULL DEFAULT 'individual',
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);

		-- Branches table: bank branches
		CREATE TABLE IF NOT EXISTS branches (
			id BIGSERIAL PRIMARY KEY,
			branch_code VARCHAR(10) NOT NULL UNIQUE,
			name VARCHAR(100) NOT NULL,
			address VARCHAR(200),
			city VARCHAR(50),
			state VARCHAR(50),
			zip_code VARCHAR(20),
			phone VARCHAR(20),
			manager_name VARCHAR(100),
			opened_at DATE,
			is_active BOOLEAN NOT NULL DEFAULT true
		);

		-- Account types table: reference data
		CREATE TABLE IF NOT EXISTS account_types (
			id SERIAL PRIMARY KEY,
			code VARCHAR(20) NOT NULL UNIQUE,
			name VARCHAR(50) NOT NULL,
			description TEXT,
			min_balance NUMERIC(15,2) NOT NULL DEFAULT 0,
			interest_rate NUMERIC(5,4) NOT NULL DEFAULT 0,
			monthly_fee NUMERIC(10,2) NOT NULL DEFAULT 0,
			is_active BOOLEAN NOT NULL DEFAULT true
		);

		-- Accounts table: main entity with FK relationships
		CREATE TABLE IF NOT EXISTS accounts (
			id BIGSERIAL PRIMARY KEY,
			account_number VARCHAR(20) NOT NULL UNIQUE,
			customer_id BIGINT REFERENCES customers(id),
			branch_id BIGINT REFERENCES branches(id),
			account_type_id INT REFERENCES account_types(id),
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100),
			balance NUMERIC(15,2) NOT NULL DEFAULT 0,
			status VARCHAR(20) NOT NULL DEFAULT 'active',
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);

		-- Transactions table: high-volume writes
		CREATE TABLE IF NOT EXISTS transactions (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL REFERENCES accounts(id),
			type VARCHAR(20) NOT NULL,
			amount NUMERIC(15,2) NOT NULL,
			description TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		);

		-- Audit log: append-only
		CREATE TABLE IF NOT EXISTS audit_log (
			id BIGSERIAL PRIMARY KEY,
			table_name VARCHAR(50) NOT NULL,
			record_id BIGINT NOT NULL,
			action VARCHAR(20) NOT NULL,
			old_values JSONB,
			new_values JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		);

		-- Indexes for customers
		CREATE INDEX IF NOT EXISTS idx_customers_customer_type ON customers(customer_type);
		CREATE INDEX IF NOT EXISTS idx_customers_city_state ON customers(city, state);
		CREATE INDEX IF NOT EXISTS idx_customers_created ON customers(created_at);

		-- Indexes for branches
		CREATE INDEX IF NOT EXISTS idx_branches_city_state ON branches(city, state);
		CREATE INDEX IF NOT EXISTS idx_branches_is_active ON branches(is_active);

		-- Indexes for accounts
		CREATE INDEX IF NOT EXISTS idx_accounts_customer ON accounts(customer_id);
		CREATE INDEX IF NOT EXISTS idx_accounts_branch ON accounts(branch_id);
		CREATE INDEX IF NOT EXISTS idx_accounts_type ON accounts(account_type_id);
		CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
		CREATE INDEX IF NOT EXISTS idx_accounts_balance ON accounts(balance);
		CREATE INDEX IF NOT EXISTS idx_accounts_created ON accounts(created_at);

		-- Indexes for transactions
		CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
		CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at);
		CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);

		-- Indexes for audit_log
		CREATE INDEX IF NOT EXISTS idx_audit_table_record ON audit_log(table_name, record_id);
		CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at);
	`

	_, err := pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating OLTP schema: %w", err)
	}

	return nil
}

// DropOLTPSchema drops all OLTP benchmark tables.
func DropOLTPSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := `
		DROP TABLE IF EXISTS audit_log CASCADE;
		DROP TABLE IF EXISTS transactions CASCADE;
		DROP TABLE IF EXISTS accounts CASCADE;
		DROP TABLE IF EXISTS account_types CASCADE;
		DROP TABLE IF EXISTS branches CASCADE;
		DROP TABLE IF EXISTS customers CASCADE;
	`

	_, err := pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("dropping OLTP schema: %w", err)
	}

	return nil
}

// SeedOLTPData populates the OLTP tables with test data using INSERT batches.
// Scale factor 1 = 5K customers, 50 branches, 10K accounts, 100K transactions.
// For large scale factors (> 10), use PreloadManager with COPY instead.
func SeedOLTPData(ctx context.Context, pool *pgxpool.Pool, seed int64, scale int) error {
	if scale < 1 {
		scale = 1
	}

	rng := rand.New(rand.NewSource(seed))

	// Seed reference data first (static)
	if err := seedAccountTypes(ctx, pool); err != nil {
		return err
	}

	// Seed branches
	numBranches := branchesPerScale * scale
	if err := seedBranches(ctx, pool, rng, numBranches); err != nil {
		return err
	}

	// Seed customers
	numCustomers := customersPerScale * scale
	if err := seedCustomers(ctx, pool, rng, numCustomers); err != nil {
		return err
	}

	// Seed accounts with FK references
	numAccounts := accountsPerScale * scale
	if err := seedAccounts(ctx, pool, rng, numAccounts, numCustomers, numBranches); err != nil {
		return err
	}

	// Seed transactions
	numTransactions := transactionsPerScale * scale
	if err := seedTransactions(ctx, pool, rng, numAccounts, numTransactions); err != nil {
		return err
	}

	// Analyze tables for query planner
	if _, err := pool.Exec(ctx, "ANALYZE customers, branches, account_types, accounts, transactions, audit_log"); err != nil {
		return fmt.Errorf("analyzing tables: %w", err)
	}

	return nil
}

func seedAccountTypes(ctx context.Context, pool *pgxpool.Pool) error {
	accountTypes := []struct {
		code        string
		name        string
		description string
		minBalance  float64
		interestRate float64
		monthlyFee  float64
	}{
		{"checking", "Checking Account", "Standard checking account for daily transactions", 0, 0.0001, 5.00},
		{"savings", "Savings Account", "Interest-bearing savings account", 100, 0.0150, 0.00},
		{"money_market", "Money Market Account", "High-yield money market account", 2500, 0.0250, 10.00},
		{"business", "Business Account", "Account for business entities", 1000, 0.0050, 15.00},
		{"premium", "Premium Account", "Premium account with enhanced benefits", 10000, 0.0200, 0.00},
	}

	for _, at := range accountTypes {
		_, err := pool.Exec(ctx,
			`INSERT INTO account_types (code, name, description, min_balance, interest_rate, monthly_fee)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (code) DO NOTHING`,
			at.code, at.name, at.description, at.minBalance, at.interestRate, at.monthlyFee)
		if err != nil {
			return fmt.Errorf("inserting account type %s: %w", at.code, err)
		}
	}

	return nil
}

func seedBranches(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, count int) error {
	const batchSize = 100

	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}

	for i := 0; i < count; i += batchSize {
		batch := batchSize
		if i+batch > count {
			batch = count - i
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}

		for j := 0; j < batch; j++ {
			idx := i + j + 1
			branchCode := fmt.Sprintf("BR%04d", idx)
			name := fmt.Sprintf("Branch %d", idx)
			cityIdx := rng.Intn(len(cities))
			address := fmt.Sprintf("%d Main St", rng.Intn(9999)+1)
			zipCode := fmt.Sprintf("%05d", rng.Intn(99999))
			phone := fmt.Sprintf("555-%03d-%04d", rng.Intn(999), rng.Intn(9999))
			managerName := fmt.Sprintf("Manager %d", idx)

			_, err := tx.Exec(ctx,
				`INSERT INTO branches (branch_code, name, address, city, state, zip_code, phone, manager_name, opened_at, is_active)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() - INTERVAL '1 year' * $9, $10)
				 ON CONFLICT (branch_code) DO NOTHING`,
				branchCode, name, address, cities[cityIdx], states[cityIdx], zipCode, phone, managerName, rng.Intn(20), true)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("inserting branch %d: %w", idx, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing branches batch: %w", err)
		}
	}

	return nil
}

func seedCustomers(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, count int) error {
	const batchSize = 1000

	firstNames := []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	customerTypes := []string{"individual", "business"}
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"}

	for i := 0; i < count; i += batchSize {
		batch := batchSize
		if i+batch > count {
			batch = count - i
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}

		for j := 0; j < batch; j++ {
			idx := i + j + 1
			customerNum := fmt.Sprintf("CUS%012d", idx)
			firstName := firstNames[rng.Intn(len(firstNames))]
			lastName := lastNames[rng.Intn(len(lastNames))]
			email := fmt.Sprintf("%s.%s%d@example.com", firstName, lastName, idx)
			phone := fmt.Sprintf("555-%03d-%04d", rng.Intn(999), rng.Intn(9999))
			address := fmt.Sprintf("%d Oak St", rng.Intn(9999)+1)
			cityIdx := rng.Intn(len(cities))
			zipCode := fmt.Sprintf("%05d", rng.Intn(99999))
			custType := customerTypes[rng.Intn(len(customerTypes))]

			_, err := tx.Exec(ctx,
				`INSERT INTO customers (customer_number, first_name, last_name, email, phone, address, city, state, zip_code, customer_type)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				 ON CONFLICT (customer_number) DO NOTHING`,
				customerNum, firstName, lastName, email, phone, address, cities[cityIdx], states[cityIdx], zipCode, custType)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("inserting customer %d: %w", idx, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing customers batch: %w", err)
		}
	}

	return nil
}

func seedAccounts(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, count, numCustomers, numBranches int) error {
	const batchSize = 1000

	statuses := []string{"active", "inactive", "suspended", "pending"}
	numAccountTypes := accountTypesStatic

	for i := 0; i < count; i += batchSize {
		batch := batchSize
		if i+batch > count {
			batch = count - i
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}

		for j := 0; j < batch; j++ {
			idx := i + j + 1
			accountNum := fmt.Sprintf("ACC%012d", idx)
			name := fmt.Sprintf("Account %d", idx)
			email := fmt.Sprintf("account%d@example.com", idx)
			balance := float64(rng.Intn(100000)) + rng.Float64()
			status := statuses[rng.Intn(len(statuses))]

			// Assign FK references
			customerID := rng.Intn(numCustomers) + 1
			branchID := rng.Intn(numBranches) + 1
			accountTypeID := rng.Intn(numAccountTypes) + 1

			_, err := tx.Exec(ctx,
				`INSERT INTO accounts (account_number, customer_id, branch_id, account_type_id, name, email, balance, status)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				 ON CONFLICT (account_number) DO NOTHING`,
				accountNum, customerID, branchID, accountTypeID, name, email, balance, status)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("inserting account %d: %w", idx, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing accounts batch: %w", err)
		}
	}

	return nil
}

func seedTransactions(ctx context.Context, pool *pgxpool.Pool, rng *rand.Rand, numAccounts, count int) error {
	const batchSize = 5000

	txTypes := []string{"deposit", "withdrawal", "transfer", "fee", "interest"}

	for i := 0; i < count; i += batchSize {
		batch := batchSize
		if i+batch > count {
			batch = count - i
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}

		for j := 0; j < batch; j++ {
			accountID := rng.Intn(numAccounts) + 1
			txType := txTypes[rng.Intn(len(txTypes))]
			amount := float64(rng.Intn(10000)) + rng.Float64()
			if txType == "withdrawal" || txType == "fee" {
				amount = -amount
			}
			desc := fmt.Sprintf("%s transaction #%d", txType, i+j+1)

			_, err := tx.Exec(ctx,
				`INSERT INTO transactions (account_id, type, amount, description)
				 VALUES ($1, $2, $3, $4)`,
				accountID, txType, amount, desc)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("inserting transaction %d: %w", i+j+1, err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing transactions batch: %w", err)
		}
	}

	return nil
}

// GetTableStats returns counts for all tables.
func GetTableStats(ctx context.Context, pool *pgxpool.Pool) (*TableStats, error) {
	stats := &TableStats{}

	queries := []struct {
		table string
		dest  *int64
	}{
		{"customers", &stats.Customers},
		{"branches", &stats.Branches},
		{"account_types", &stats.AccountTypes},
		{"accounts", &stats.Accounts},
		{"transactions", &stats.Transactions},
		{"audit_log", &stats.AuditLog},
	}

	for _, q := range queries {
		err := pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", q.table)).Scan(q.dest)
		if err != nil {
			return nil, fmt.Errorf("counting %s: %w", q.table, err)
		}
	}

	return stats, nil
}

// GetAccountCount returns the number of accounts in the database.
func GetAccountCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM accounts").Scan(&count)
	return count, err
}

// GetTransactionCount returns the number of transactions in the database.
func GetTransactionCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM transactions").Scan(&count)
	return count, err
}

// GetCustomerCount returns the number of customers in the database.
func GetCustomerCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM customers").Scan(&count)
	return count, err
}

// GetBranchCount returns the number of branches in the database.
func GetBranchCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM branches").Scan(&count)
	return count, err
}
