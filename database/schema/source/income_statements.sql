CREATE TABLE IF NOT EXISTS income_statements (
    instrument TEXT NOT NULL,
    fiscal_date DATE NOT NULL,
    frequency TEXT CHECK (frequency IN ('annual', 'quarterly')) NOT NULL,
    
    total_revenue NUMERIC,
    cost_of_revenue NUMERIC,
    cost_of_goods_and_services_sold NUMERIC,
    gross_profit NUMERIC,
    operating_income NUMERIC,
    operating_expenses NUMERIC,
    research_and_development NUMERIC,
    selling_general_and_administrative NUMERIC,
    
    depreciation NUMERIC,
    depreciation_and_amortization NUMERIC,
    ebit NUMERIC,
    ebitda NUMERIC,
    
    income_before_tax NUMERIC,
    income_tax_expense NUMERIC,
    net_income NUMERIC,
    net_income_from_continuing_operations NUMERIC,
    comprehensive_income_net_of_tax NUMERIC,
    
    interest_expense NUMERIC,
    interest_income NUMERIC,
    interest_and_debt_expense NUMERIC,
    net_interest_income NUMERIC,
    investment_income_net NUMERIC,
    non_interest_income NUMERIC,
    other_non_operating_income NUMERIC,

    created_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (instrument, fiscal_date, frequency)
);