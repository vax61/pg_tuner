package profile

// QueryType represents the type of query operation.
type QueryType string

const (
	QueryTypeRead  QueryType = "read"
	QueryTypeWrite QueryType = "write"
)

// QueryTemplate defines a parameterized query for workload generation.
type QueryTemplate struct {
	Name        string    `yaml:"name"`
	SQL         string    `yaml:"sql"`
	Type        QueryType `yaml:"type"`
	Weight      int       `yaml:"weight"`
	Description string    `yaml:"description,omitempty"`
}

// OLTPQueries contains the standard OLTP query templates with JOIN support.
// Weight distribution:
//   - Point lookup: 30%
//   - Range select: 12%
//   - Insert TX: 18%
//   - Update balance: 12%
//   - 2-way JOIN: 19%
//   - 3-way+ JOIN: 9%
var OLTPQueries = []QueryTemplate{
	// === Point Lookups (30%) ===
	{
		Name:        "point_select",
		SQL:         "SELECT id, account_number, name, email, balance, status, created_at, updated_at FROM accounts WHERE id = $1",
		Type:        QueryTypeRead,
		Weight:      30,
		Description: "Point lookup by primary key",
	},

	// === Range Selects (12%) ===
	{
		Name:        "range_select",
		SQL:         "SELECT id, account_number, name, balance, status FROM accounts WHERE balance BETWEEN $1 AND $2 ORDER BY balance LIMIT 100",
		Type:        QueryTypeRead,
		Weight:      12,
		Description: "Range scan on balance with limit",
	},

	// === Writes (30% total) ===
	{
		Name:        "insert_tx",
		SQL:         "INSERT INTO transactions (account_id, type, amount, description) VALUES ($1, $2, $3, $4) RETURNING id",
		Type:        QueryTypeWrite,
		Weight:      18,
		Description: "Insert new transaction",
	},
	{
		Name:        "update_balance",
		SQL:         "UPDATE accounts SET balance = balance + $1, updated_at = NOW() WHERE id = $2 RETURNING balance",
		Type:        QueryTypeWrite,
		Weight:      12,
		Description: "Update account balance",
	},

	// === 2-way JOINs (19% total) ===
	{
		Name: "customer_accounts",
		SQL: `SELECT c.id, c.customer_number, c.first_name, c.last_name, c.customer_type,
		             a.id as account_id, a.account_number, a.balance, a.status
		      FROM customers c
		      JOIN accounts a ON c.id = a.customer_id
		      WHERE c.id = $1`,
		Type:        QueryTypeRead,
		Weight:      8,
		Description: "Get customer with all their accounts (2-way JOIN)",
	},
	{
		Name: "account_transactions",
		SQL: `SELECT a.id, a.account_number, a.balance, a.status,
		             t.id as tx_id, t.type, t.amount, t.created_at
		      FROM accounts a
		      JOIN transactions t ON a.id = t.account_id
		      WHERE a.id = $1
		      ORDER BY t.created_at DESC
		      LIMIT 50`,
		Type:        QueryTypeRead,
		Weight:      8,
		Description: "Get account with recent transactions (2-way JOIN)",
	},
	{
		Name: "branch_accounts",
		SQL: `SELECT b.id, b.branch_code, b.name as branch_name, b.city, b.state,
		             COUNT(a.id) as account_count,
		             COALESCE(SUM(a.balance), 0) as total_balance,
		             COALESCE(AVG(a.balance), 0) as avg_balance
		      FROM branches b
		      LEFT JOIN accounts a ON b.id = a.branch_id
		      WHERE b.id = $1
		      GROUP BY b.id, b.branch_code, b.name, b.city, b.state`,
		Type:        QueryTypeRead,
		Weight:      3,
		Description: "Get branch with account aggregations (2-way JOIN with GROUP BY)",
	},

	// === 3-way+ JOINs (9% total) ===
	{
		Name: "customer_tx_summary",
		SQL: `SELECT c.id, c.customer_number, c.first_name, c.last_name,
		             COUNT(DISTINCT a.id) as account_count,
		             COUNT(t.id) as tx_count,
		             COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END), 0) as total_deposits,
		             COALESCE(SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END), 0) as total_withdrawals
		      FROM customers c
		      JOIN accounts a ON c.id = a.customer_id
		      LEFT JOIN transactions t ON a.id = t.account_id
		      WHERE c.id = $1
		      GROUP BY c.id, c.customer_number, c.first_name, c.last_name`,
		Type:        QueryTypeRead,
		Weight:      4,
		Description: "Customer transaction summary (3-way JOIN with aggregation)",
	},
	{
		Name: "branch_tx_summary",
		SQL: `SELECT b.id, b.branch_code, b.name as branch_name,
		             COUNT(DISTINCT a.id) as account_count,
		             COUNT(t.id) as tx_count,
		             COALESCE(SUM(a.balance), 0) as total_balance
		      FROM branches b
		      JOIN accounts a ON b.id = a.branch_id
		      LEFT JOIN transactions t ON a.id = t.account_id
		      WHERE b.id = $1
		      GROUP BY b.id, b.branch_code, b.name`,
		Type:        QueryTypeRead,
		Weight:      2,
		Description: "Branch transaction summary (3-way JOIN with aggregation)",
	},
	{
		Name: "customer_audit_trail",
		SQL: `SELECT c.id, c.customer_number, c.first_name, c.last_name,
		             a.account_number,
		             al.action, al.old_values, al.new_values, al.created_at
		      FROM customers c
		      JOIN accounts a ON c.id = a.customer_id
		      LEFT JOIN audit_log al ON al.table_name = 'accounts' AND al.record_id = a.id
		      WHERE c.id = $1
		      ORDER BY al.created_at DESC NULLS LAST
		      LIMIT 100`,
		Type:        QueryTypeRead,
		Weight:      1,
		Description: "Customer audit trail (3-way JOIN with audit_log)",
	},
	{
		Name: "full_customer_report",
		SQL: `SELECT c.id, c.customer_number, c.first_name, c.last_name, c.customer_type,
		             b.branch_code, b.name as branch_name, b.city as branch_city,
		             at.code as account_type_code, at.name as account_type_name,
		             a.account_number, a.balance, a.status,
		             COUNT(t.id) as tx_count,
		             COALESCE(SUM(t.amount), 0) as net_amount
		      FROM customers c
		      JOIN accounts a ON c.id = a.customer_id
		      JOIN branches b ON a.branch_id = b.id
		      JOIN account_types at ON a.account_type_id = at.id
		      LEFT JOIN transactions t ON a.id = t.account_id
		      WHERE c.id = $1
		      GROUP BY c.id, c.customer_number, c.first_name, c.last_name, c.customer_type,
		               b.branch_code, b.name, b.city,
		               at.code, at.name,
		               a.account_number, a.balance, a.status
		      ORDER BY a.balance DESC`,
		Type:        QueryTypeRead,
		Weight:      1,
		Description: "Full customer report (4-way JOIN with all related entities)",
	},
	{
		Name: "complex_join",
		SQL: `SELECT a.id, a.account_number, a.name, a.balance,
		             COUNT(t.id) as tx_count,
		             COALESCE(SUM(t.amount), 0) as total_amount
		      FROM accounts a
		      LEFT JOIN transactions t ON a.id = t.account_id
		      WHERE a.status = $1
		      GROUP BY a.id, a.account_number, a.name, a.balance
		      ORDER BY total_amount DESC
		      LIMIT 50`,
		Type:        QueryTypeRead,
		Weight:      1,
		Description: "Legacy complex join with aggregation",
	},
}

// GetQueryByName returns a query template by name.
func GetQueryByName(name string) *QueryTemplate {
	for i := range OLTPQueries {
		if OLTPQueries[i].Name == name {
			return &OLTPQueries[i]
		}
	}
	return nil
}

// GetTotalWeight returns the sum of all query weights.
func GetTotalWeight() int {
	total := 0
	for _, q := range OLTPQueries {
		total += q.Weight
	}
	return total
}

// SelectQueryByWeight selects a query based on weighted random selection.
// The value should be in range [0, totalWeight).
func SelectQueryByWeight(value int) *QueryTemplate {
	cumulative := 0
	for i := range OLTPQueries {
		cumulative += OLTPQueries[i].Weight
		if value < cumulative {
			return &OLTPQueries[i]
		}
	}
	// Fallback to last query
	return &OLTPQueries[len(OLTPQueries)-1]
}

// GetReadQueries returns only read queries.
func GetReadQueries() []QueryTemplate {
	var queries []QueryTemplate
	for _, q := range OLTPQueries {
		if q.Type == QueryTypeRead {
			queries = append(queries, q)
		}
	}
	return queries
}

// GetWriteQueries returns only write queries.
func GetWriteQueries() []QueryTemplate {
	var queries []QueryTemplate
	for _, q := range OLTPQueries {
		if q.Type == QueryTypeWrite {
			queries = append(queries, q)
		}
	}
	return queries
}

// GetJoinQueries returns only JOIN queries (2-way and 3-way+).
func GetJoinQueries() []QueryTemplate {
	joinQueries := []string{
		"customer_accounts",
		"account_transactions",
		"branch_accounts",
		"customer_tx_summary",
		"branch_tx_summary",
		"customer_audit_trail",
		"full_customer_report",
		"complex_join",
	}

	var queries []QueryTemplate
	for _, q := range OLTPQueries {
		for _, jq := range joinQueries {
			if q.Name == jq {
				queries = append(queries, q)
				break
			}
		}
	}
	return queries
}
