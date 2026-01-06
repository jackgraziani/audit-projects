"""
Project: Automated Audit Reconciliation Engine
Author: Jack Graziani ( Jan 2026)
Dependencies: pandas, numpy, xlsxwriter, thefuzz
"""

import pandas as pd
import numpy as np
import datetime
from thefuzz import fuzz
import xlsxwriter
import random
import uuid
from faker import Faker
import os

# --- CONFIGURATION ---
COMPANIES = ['NVIDIA Corporation', 'Apple Inc.', 'Alphabet, Inc.', 'Microsoft Corporation', 'Amazon.com, Inc.', 'Broadcom Inc.', 'Meta Platforms, Inc.', 'Tesla, Inc.', 'Berkshire Hathaway Inc.', 'Eli Lilly and Company', 'Walmart Inc.', 'JPMorgan Chase & Co.', 'Visa Inc.', 'Oracle Corporation', 'Exxon Mobil Corporation', 'Mastercard Incorporated', 'Johnson & Johnson', 'Netflix, Inc.', 'Bank of America Corporation', 'AbbVie Inc.', 'Palantir Technologies Inc.', 'Costco Wholesale Corporation', 'Advanced Micro Devices, Inc.', 'Micron Technology, Inc.', 'The Home Depot, Inc.', 'GE Aerospace', 'The Procter & Gamble Company', 'Chevron Corporation', 'UnitedHealth Group Incorporated', 'Cisco Systems, Inc.', 'Wells Fargo & Company', 'The Coca-Cola Company', 'Morgan Stanley', 'The Goldman Sachs Group, Inc.', 'Caterpillar Inc.', 'International Business Machines Corporation', 'Merck & Co., Inc.', 'American Express Company', 'RTX Corporation', 'Philip Morris International Inc.', 'Salesforce, Inc.', 'Lam Research Corporation', 'T-Mobile US, Inc.', 'Thermo Fisher Scientific Inc.', "McDonald's Corporation", 'Abbott Laboratories', 'Applied Materials, Inc.', 'Citigroup Inc.', 'AppLovin Corporation', 'Linde plc', 'The Walt Disney Company', 'Intuitive Surgical, Inc.', 'PepsiCo, Inc.', 'Intel Corporation', 'Blackstone Inc.', 'GE Vernova Inc.', 'QUALCOMM Incorporated', 'The Charles Schwab Corporation', 'BlackRock, Inc.', 'Amgen Inc.', 'Intuit Inc.', 'AT&T Inc.', 'The Boeing Company', 'Uber Technologies, Inc.', 'Booking Holdings Inc.', 'The TJX Companies, Inc.', 'Amphenol Corporation', 'Verizon Communications Inc.', 'NextEra Energy, Inc.', 'Arista Networks, Inc.', 'KLA Corporation', 'Danaher Corporation', 'Texas Instruments Incorporated', 'Accenture plc', 'Capital One Financial Corporation', 'S&P Global Inc.', 'ServiceNow, Inc.', 'Gilead Sciences, Inc.', 'Pfizer Inc.', 'Boston Scientific Corporation', 'Adobe Inc.', "Lowe's Companies, Inc.", 'Union Pacific Corporation', 'Analog Devices, Inc.', 'Stryker Corporation', 'Welltower Inc.', 'Eaton Corporation plc', 'Deere & Company', 'Palo Alto Networks, Inc.', 'The Progressive Corporation', 'Honeywell International Inc.', 'Medtronic plc', 'Prologis, Inc.', 'Chubb Limited', 'ConocoPhillips', 'Lockheed Martin Corporation', 'KKR & Co. Inc.', 'Vertex Pharmaceuticals Incorporated', 'Constellation Energy Corporation', 'CrowdStrike Holdings, Inc.', 'Parker-Hannifin Corporation', 'Newmont Corporation', 'Bristol-Myers Squibb Company', 'Comcast Corporation', 'HCA Healthcare, Inc.', 'Robinhood Markets, Inc.', 'Automatic Data Processing, Inc.', 'CVS Health Corporation', 'McKesson Corporation', 'CME Group Inc.', 'Altria Group, Inc.', 'The Southern Company', 'Starbucks Corporation', 'DoorDash, Inc.', 'NIKE, Inc.', 'General Dynamics Corporation', 'Synopsys, Inc.', 'Duke Energy Corporation', 'Intercontinental Exchange, Inc.', 'Marsh & McLennan Companies, Inc.', 'Moody\'s Corporation', 'Trane Technologies plc', 'Waste Management, Inc.', 'Carvana Co.', '3M Company', 'United Parcel Service, Inc.', 'Howmet Aerospace Inc.', 'Apollo Global Management, Inc.', 'CRH plc', 'Dell Technologies Inc.', 'Cadence Design Systems, Inc.', 'Marriott International, Inc.', 'U.S. Bancorp', 'Northrop Grumman Corporation', 'The PNC Financial Services Group, Inc.', 'American Tower Corporation', 'The Bank of New York Mellon Corporation', 'The Sherwin-Williams Company', 'Airbnb, Inc.', 'Regeneron Pharmaceuticals, Inc.', 'Elevance Health, Inc.', 'Corning Incorporated', 'Royal Caribbean Cruises Ltd.', 'TransDigm Group Incorporated', 'Emerson Electric Co.', "O'Reilly Automotive, Inc.", 'General Motors Company', 'Equinix, Inc.', 'Johnson Controls International plc', 'Freeport-McMoRan Inc.', 'The Cigna Group', 'Monster Beverage Corporation', 'Ecolab Inc.', 'The Williams Companies, Inc.', 'Aon plc', 'Cintas Corporation', 'Illinois Tool Works Inc.', 'Cummins Inc.', 'Warner Bros. Discovery, Inc.', 'Simon Property Group, Inc.', 'Mondelez International, Inc.', 'FedEx Corporation', 'TE Connectivity plc', 'Hilton Worldwide Holdings Inc.', 'CSX Corporation', 'Arthur J. Gallagher & Co.', 'Cencora, Inc.', 'Quanta Services, Inc.', 'Republic Services, Inc.', 'Norfolk Southern Corporation', 'Western Digital Corporation', 'Coinbase Global, Inc.', 'Truist Financial Corporation', 'The Travelers Companies, Inc.', 'Motorola Solutions, Inc.', 'Seagate Technology Holdings plc', 'Colgate-Palmolive Company', 'American Electric Power Company, Inc.', 'Kinder Morgan, Inc.', 'Autodesk, Inc.', 'SLB N.V.', 'Ross Stores, Inc.', 'PACCAR Inc', 'Sempra', 'EOG Resources, Inc.', 'Fortinet, Inc.', 'Aflac Incorporated']
# Standardizing: Negative = Money Out, Positive = Money In
# Save output to the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))

output_filename = os.path.join(script_dir, "Audit_Exception_Report.xlsx")

def fuzzify_string(text):
    """adds noise to a string for testing fuzzy matching"""
    if random.choice([True, False]):
        # Remove vowels randomly or truncate
        return text.replace('e', '').replace('a', '')[:10]
    return text.upper()

def generate_audit_data():
    """
    Generates fake General Ledger and Bank CSVs with intentional messiness.
    """
    fake = Faker()
    print("Generating synthetic data...")

    data = []
    # Create 1000 base transactions
    for i in range(1000):
        is_expense = random.choice([True, True, False]) # More expenses than deposits usually
        amt = round(random.uniform(10, 5000), 2)
        if is_expense:
            amt = amt * -1
        
        row = {
            'uid': str(uuid.uuid4()),
            'date': fake.date_between(start_date='-30d', end_date='today'),
            'desc': random.choice(COMPANIES),
            'amount': amt,
            'type': 'regular'
        }
        data.append(row)

    master_df = pd.DataFrame(data)

    # --- CREATE GL DATA (Internal Records) ---
    gl_df = master_df.copy()
    # Add Uncleared Checks (In GL, not in Bank)
    for _ in range(15):
        gl_df.loc[len(gl_df)] = {
            'uid': str(uuid.uuid4()),
            'date': fake.date_between(start_date='-30d', end_date='today'),
            'desc': 'Check #' + str(random.randint(1000,9999)),
            'amount': round(random.uniform(-500, -50), 2),
            'type': 'uncleared'
        }
    
    # Introduce Formatting Inconsistencies to GL
    # Convert some dates to strings, add currency symbols to amounts
    gl_df['amount'] = gl_df['amount'].apply(lambda x: f"${x:,.2f}" if random.random() > 0.5 else x)
    gl_df['desc'] = gl_df['desc'].apply(lambda x: f" {x}  " if random.random() > 0.5 else x) # Trailing whitespace

    # --- CREATE BANK DATA (External Records) ---
    # Take the master, drop the "Uncleared" items (simulating they haven't cashed yet)
    # But drop a few random regular ones to create "Bank Errors" or "Timing differences"
    bank_df = master_df.sample(frac=0.95).copy()

    # Add Bank Fees (In Bank, not in GL)
    for _ in range(10):
        bank_df.loc[len(bank_df)] = {
            'uid': str(uuid.uuid4()),
            'date': fake.date_between(start_date='-30d', end_date='today'),
            'desc': 'MONTHLY SERVICE FEE',
            'amount': -15.00,
            'type': 'fee'
        }

    # Mess up descriptions for Fuzzy Match testing
    bank_df['desc'] = bank_df['desc'].apply(lambda x: fuzzify_string(str(x)) if random.random() > 0.8 else x)

    # Create a "Many-to-One" scenario
    # GL has 3 small transactions, Bank has 1 lump sum
    subset_sum = -300.00
    date_shared = fake.date_between(start_date='-30d', end_date='today')
    
    # Add 3 items to GL
    gl_df.loc[len(gl_df)] = {'uid': 'GL_BATCH_1', 'date': date_shared, 'desc': 'Part A', 'amount': -100.00, 'type': 'batch'}
    gl_df.loc[len(gl_df)] = {'uid': 'GL_BATCH_2', 'date': date_shared, 'desc': 'Part B', 'amount': -100.00, 'type': 'batch'}
    gl_df.loc[len(gl_df)] = {'uid': 'GL_BATCH_3', 'date': date_shared, 'desc': 'Part C', 'amount': -100.00, 'type': 'batch'}

    # Add 1 item to Bank
    bank_df.loc[len(bank_df)] = {'uid': 'BANK_BATCH_1', 'date': date_shared, 'desc': 'BATCH SETTLEMENT TOTAL', 'amount': -300.00, 'type': 'batch'}

    # Save to CSV to simulate "Ingestion"
    gl_df.to_csv('raw_gl_data.csv', index=False)
    bank_df.to_csv('raw_bank_data.csv', index=False)
    return 'raw_gl_data.csv', 'raw_bank_data.csv'


def clean_currency(val):
    """Parses $1,200.50, (500.00), and 500.00 into floats"""
    if isinstance(val, (float, int)):
        return float(val)
    val = str(val).strip()
    val = val.replace('$', '').replace(',', '')
    val = val.replace(')', '').replace('(', '-') # Accounting format (500) -> -500
    return float(val)

def clean_dataset(df, source_name):
    """
    Standardizes columns, types, and formats.
    """
    # 1. Standardize Columns
    df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
    
    # 2. Clean Amounts
    if 'amount' in df.columns:
        df['amount'] = df['amount'].apply(clean_currency)
    
    # 3. Clean Dates
    # Coerce errors=coerce turns unparseable dates into NaT (Not a Time)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
    # 4. Clean Strings
    if 'desc' in df.columns:
        df['desc'] = df['desc'].astype(str).str.strip().str.upper()
        
    # 5. Add Source Tracker
    df['source'] = source_name
    
    # 6. Generate a unique ID for tracking within the script if one doesn't exist
    if 'uid' not in df.columns:
        df['uid'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
    return df

class ReconEngine:
    def __init__(self, gl_df, bank_df):
        self.gl = gl_df.copy()
        self.bank = bank_df.copy()
        self.matches = []
        
        # Add a temporary 'match_id' column to track status
        self.gl['match_id'] = None
        self.bank['match_id'] = None

    def _mark_match(self, gl_idxs, bank_idxs, rule_name):
        match_id = str(uuid.uuid4())[:8]
        
        # Update DataFrames
        self.gl.loc[gl_idxs, 'match_id'] = match_id
        self.bank.loc[bank_idxs, 'match_id'] = match_id
        
        # Store record for report
        # We assume 1-to-1 or Many-to-1. We grab the Amount from the Bank side for the record.
        amount = self.bank.loc[bank_idxs, 'amount'].sum()
        self.matches.append({
            'Match_ID': match_id,
            'Rule': rule_name,
            'Amount': amount,
            'GL_Count': len(gl_idxs),
            'Bank_Count': len(bank_idxs)
        })

    def layer_1_exact_match(self):
        """Matches on Date + Amount + Description (Strict)"""
        # Filter only unmatched rows
        gl_active = self.gl[self.gl['match_id'].isnull()]
        bank_active = self.bank[self.bank['match_id'].isnull()]

        # Inner Merge to find keys
        merged = pd.merge(
            gl_active.reset_index(), 
            bank_active.reset_index(), 
            on=['date', 'amount', 'desc'], 
            suffixes=('_gl', '_bk')
        )

        for _, row in merged.iterrows():
            self._mark_match([row['index_gl']], [row['index_bk']], "Layer 1: Exact")

    def layer_2_fuzzy_match(self):
        """Matches on Date + Amount + Fuzzy Description"""
        # Iterate over remaining GL items
        gl_active = self.gl[self.gl['match_id'].isnull()]
        
        for idx_gl, row_gl in gl_active.iterrows():
            # Find Bank items with same Amount and Date
            candidates = self.bank[
                (self.bank['match_id'].isnull()) & 
                (self.bank['amount'] == row_gl['amount']) &
                (self.bank['date'] == row_gl['date'])
            ]
            
            if candidates.empty:
                continue

            # Fuzzy Check
            best_score = 0
            best_idx = None
            
            for idx_bk, row_bk in candidates.iterrows():
                score = fuzz.ratio(row_gl['desc'], row_bk['desc'])
                if score > 80 and score > best_score:
                    best_score = score
                    best_idx = idx_bk
            
            if best_idx is not None:
                self._mark_match([idx_gl], [best_idx], f"Layer 2: Fuzzy ({best_score}%)")

    def layer_3_many_to_one_match(self):
        """
        Groups GL items by Date, Sums them, and looks for that Sum in Bank.
        Real world note: This usually requires a common 'Batch ID', but we will try by Date/Sum only.
        """
        gl_active = self.gl[self.gl['match_id'].isnull()]
        bank_active = self.bank[self.bank['match_id'].isnull()]

        # Group GL by Date
        gl_grouped = gl_active.groupby('date')['amount'].sum().reset_index()

        for _, group in gl_grouped.iterrows():
            target_date = group['date']
            target_sum = group['amount']

            # Find a single bank transaction that equals this sum on this date
            bank_match = bank_active[
                (bank_active['date'] == target_date) & 
                (np.isclose(bank_active['amount'], target_sum))
            ]

            if not bank_match.empty:
                # We found a match! Retrieve the individual GL indices that made up this sum
                gl_indices = gl_active[gl_active['date'] == target_date].index.tolist()
                bank_indices = [bank_match.index[0]] # Take the first match
                
                self._mark_match(gl_indices, bank_indices, "Layer 3: Many-to-One")

    def get_results(self):
        return self.gl, self.bank, pd.DataFrame(self.matches)

def generate_exception_report(gl_df, bank_df, matches_df):
    """Writes the final Excel report with formatting."""
    
    # Split into matched vs unmatched
    gl_unmatched = gl_df[gl_df['match_id'].isnull()].copy()
    bank_unmatched = bank_df[bank_df['match_id'].isnull()].copy()
    
    # Calculate variances
    gl_balance = gl_df['amount'].sum()
    bank_balance = bank_df['amount'].sum()
    variance = gl_balance - bank_balance

    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        money_fmt = workbook.add_format({'num_format': '$#,##0.00'})
        date_fmt = workbook.add_format({'num_format': 'mm/dd/yyyy'})
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        
        # --- SHEET 1: SUMMARY ---
        summary_data = {
            'Metric': ['Total GL Balance', 'Total Bank Balance', 'Variance', 'Match Rate'],
            'Value': [gl_balance, bank_balance, variance, f"{len(matches_df)} matches found"]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        worksheet = writer.sheets['Summary']
        worksheet.set_column('A:A', 20)
        worksheet.set_column('B:B', 20, money_fmt)
        
        # Conditional Format for Variance
        if abs(variance) > 0.01:
            worksheet.write('B4', variance, red_fmt)

        # --- SHEET 2: EXCEPTIONS (GL) ---
        gl_unmatched.drop(columns=['match_id']).to_excel(writer, sheet_name='GL Exceptions', index=False)
        worksheet = writer.sheets['GL Exceptions']
        worksheet.set_column('B:B', 15, date_fmt)
        worksheet.set_column('D:D', 15, money_fmt)
        worksheet.set_column('C:C', 40) # Description width

        # --- SHEET 3: EXCEPTIONS (BANK) ---
        bank_unmatched.drop(columns=['match_id']).to_excel(writer, sheet_name='Bank Exceptions', index=False)
        worksheet = writer.sheets['Bank Exceptions']
        worksheet.set_column('B:B', 15, date_fmt)
        worksheet.set_column('D:D', 15, money_fmt)
        worksheet.set_column('C:C', 40)

        # --- SHEET 4: MATCHED DETAILS ---
        if not matches_df.empty:
            matches_df.to_excel(writer, sheet_name='Matched Transactions', index=False)
            worksheet = writer.sheets['Matched Transactions']
            worksheet.set_column('C:C', 15, money_fmt)

    print(f"Report generated successfully: {output_filename}")


if __name__ == "__main__":
    # 1. Ingest Data (Simulated)
    f1, f2 = generate_audit_data()
    
    gl_raw = pd.read_csv(f1)
    bank_raw = pd.read_csv(f2)
    
    # 2. Clean Data (ETL)
    gl_clean = clean_dataset(gl_raw, "GL")
    bank_clean = clean_dataset(bank_raw, "Bank")
    
    # 3. Reconcile
    engine = ReconEngine(gl_clean, bank_clean)
    engine.layer_1_exact_match()
    engine.layer_2_fuzzy_match()
    engine.layer_3_many_to_one_match()
    
    # 4. Reporting
    gl_final, bank_final, matches = engine.get_results()
    generate_exception_report(gl_final, bank_final, matches)
    
    # Cleanup dummy files
    if os.path.exists(f1): os.remove(f1)
    if os.path.exists(f2): os.remove(f2)