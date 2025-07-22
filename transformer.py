import extractor as ex
from typing import Dict, Any
from collections import defaultdict
from datetime import datetime 
import pandas as pd
from google import genai
import os
import dotenv
from loader import get_snowflake_connection
import json
import uuid

dotenv.load_dotenv()
PROD = os.getenv('PROD') == 'True'

# AI Summary --> Local Testing only 
# 
# If we're in prod the service account should be added to the deployment service
if not PROD:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('CREDENTIALS')

try:
    client = genai.Client(
            vertexai=True, project=os.getenv('PROJECT_ID'), location='us-central1'
        )
except Exception as e:
    pass 

# Function to check if there's already a begin - end date report 
def check_report_exists(begin_date, end_date, details, fi_summary):
    """
        We don't want to insert if the report aka summary already exists.. If there's a different begin and end date then its good to insert 

        However, there's a major flaw here... What if there are more files in the middle?? 

        Solution:
        We check if begin_date and end_date exists, if it does did the transaction increase? If so we alter the current begin and end date report 
        
        Else Begin_date and End_date actually doesn't exist meaning this is a new report 
    """
    # Loading into Snowflake 
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Checking if begin and end date exists 
    check_query = """
        SELECT * FROM file_details
        WHERE begin_date = %s and end_date = %s
    """
    try:
        cursor.execute(check_query, (begin_date, end_date))
        exists = cursor.fetchone() 
        if exists:
            # Check if the Transaction is less than our new Transaction counts 
            new_transaction_cnt = details['total_transactions']
            # Index 3 is for the Details in our tuple...
            details_variant = json.loads(exists[3])
            details_transactions = details_variant['total_transactions']

            if new_transaction_cnt > details_transactions:
                # One or More files were uploaded between the date range so we need to ALTER the current record 
                # make sure our function also closes our conn + cursor after updating 
                update_snowflake(conn, cursor, begin_date, end_date, details, fi_summary)
            else:
                print('No new files to update...')
        else:
            # Report doesnt' exists therefore we need to insert 
            # insert_to_snowflake will close our the conn and cursor after inserting 
            insert_to_snowflake(conn, cursor, begin_date, end_date, details, fi_summary)
    except Exception as e:
        print('Error Check Report')
        print(e)
    finally:
        # Ensure the cursor and connection are always closed
        cursor.close()
        conn.close()

def update_snowflake(conn, cursor, begin_date, end_date, details, fi_summary):
    try:
        update_query = """
            UPDATE file_details
            SET details = PARSE_JSON(%s), fi_summary = %s
            WHERE begin_date = %s AND end_date = %s
        """

        cursor.execute(update_query, (
            json.dumps(details),
            fi_summary,
            begin_date,
            end_date
        ))
        conn.commit()
        print("Report successfully updated.")
    except Exception as e:
        print('Error in updating report')
        print(e)



def insert_to_snowflake(conn, cursor, begin_date, end_date, details, fi_summary, id=str(uuid.uuid4())):

    # Ideally you would have a company_name / company_id but since this is just for one big client we don't need to worrk about inserting for specific companies 
    # https://community.snowflake.com/s/article/INSERT-using-data-in-JSON-format-fails-with-the-error-Invalid-expression-PARSEJSON
    #
    # Here we use IIS not IIV --> Normally we could INSERT INTO TABLE_NAME VALUES ()
    # But since we're working with VARIANT which need PARSE_JSON() to read our json.dumps(details)....
    # We need to do INSERT INTO TABLE_NAME SELECT 
    insert_query = """
        INSERT INTO file_details (id, begin_date, end_date, details, fi_summary)
        SELECT
        %s,
        %s,
        %s,
        PARSE_JSON(%s),
        %s
    """

    try:
        cursor.execute(insert_query, (
            id, 
            begin_date,
            end_date,
            json.dumps(details),
            fi_summary
        ))
        
        # Committing our insert 
        conn.commit()
        print("✅ Inserted file details successfully.")
    except Exception as e:
        print('Error inserting into Snowflake')
        print(e)


def transform_summary() -> Dict[str, Any]:
    """
        Note: Ideally, you would have your application for multiple CLIENTS; however, this application doesn't have a model to specific companies.

        In the case where you have multiple clients, you could always group them up then based on the grouped clients, provide total spent for each.

        In our case, we would sum up all the total spent because we're focused on ONE big client...
    """
    all_uploaded_files = ex.get_uploaded_files() 

    # Expenses & transactions 
    total_expenses = 0
    total_transactions = 0  

    # Default Dictionary provides a default value 
    category_total = defaultdict(float)
    vendor_total = defaultdict(float)

    # Unique vendor & category 
    category_set = set() 
    vendor_set = set() 

    # Date Range
    begin_date, end_date = None, None
    date_format = '%Y-%m-%d'

    # Category & Vendor Data for Averaging 
    category_data = [] 
    vendor_data = []
    dates = []

    for file in all_uploaded_files:
        summary = ex.get_summary(file)
        if not summary:
            continue 

        # Tracking Date 
        begin_date_datetime_format = datetime.strptime(summary.get('begin_date', None), date_format).date()
        if begin_date is None:
            begin_date = begin_date_datetime_format
        else:
            # Compare current begin_date 
            begin_date = min(begin_date, begin_date_datetime_format)

        end_date_datetime_format = datetime.strptime(summary.get('end_date', None), date_format).date()
        if end_date is None:
            end_date = end_date_datetime_format
        else:
            # Compare current begin_date 
            end_date = max(end_date, end_date_datetime_format)

        total_expenses += float(summary.get('total_spent', 0))
        total_transactions += summary.get('total_transactions', 0)
        
        # Looping through category 
        category_row = []
        for category, amount in summary['spending_per_category'].items():
            category_total[category] += round(float(amount),2)
            category_set.add(category)
            category_row.append(amount)
        
        vendor_row = []
        # Looping through vendor 
        for vendor, amount in summary['spending_per_vendor'].items():
            vendor_total[vendor] += round(float(amount),2)
            vendor_set.add(vendor)
            vendor_row.append(amount)

        # Adding the rows into our main array to generate a DF for averaging 
        category_data.append(category_row)
        vendor_data.append(vendor_row)
        dates.append(datetime.strptime(summary.get('end_date', None), date_format).date())

    sorted_category_total = sorted(category_total.items(), key=lambda cat: cat[1])
    sorted_vendor_total = sorted(vendor_total.items(), key=lambda vendor: vendor[1])

    # Dataframe for averaging + insightful details for our AI 
    category_df = pd.DataFrame(category_data, columns=list(category_set), index=pd.to_datetime(dates))
    vendor_df = pd.DataFrame(vendor_data, columns=list(vendor_set), index=pd.to_datetime(dates))

    # Percent Change (Category & Vendor)
    category_pct_change = category_df.pct_change().fillna(0).round(2).iloc[-1].to_dict()
    vendor_pct_change = vendor_df.pct_change().fillna(0).round(2).iloc[-1].to_dict()

    # Average (Mean) (Category & Vendor)
    category_avg = category_df.mean().round(2).to_dict()
    vendor_avg = vendor_df.mean().round(2).to_dict()
    
    ai_fi_summary = ''
    response =  {
        'total_spent': round(total_expenses, 2),
        'total_transactions': total_transactions,
        'unique_categories': sorted(list(category_set)),
        'unique_vendors': sorted(list(vendor_set)),
        'spending_per_category': dict(sorted_category_total),
        'pct_change_category': category_pct_change,
        'avg_category': category_avg, 
        'spending_per_vendor': dict(sorted_vendor_total),
        'pct_change_vendor': vendor_pct_change,
        'avg_vendor': vendor_avg,
        'top_5_vendors': dict(sorted_vendor_total[:5]),
        'begin_date': begin_date.isoformat(),
        'end_date': end_date.isoformat()
    }

    try:
        prompt = f"""
        You are a financial analyst assistant. Given the spending data from {response['begin_date']} to {response['end_date']}, generate a professional, smart, and concise summary of key financial insights.

        This summary will be **copied and pasted directly to my boss**, so it must be clear, business-appropriate, and free of fluff. Do not include the raw input data or repeat numbers unless necessary to support a key insight.

        Here is the structured data:
        - Total Spent: ${response['total_spent']}
        - Total Transactions: {response['total_transactions']}
        - Unique Categories: {', '.join(response['unique_categories'])}
        - Unique Vendors: {', '.join(response['unique_vendors'])}
        - Spending Per Category: {response['spending_per_category']}
        - Percentage Change in Category Spending (from previous): {response['pct_change_category']}
        - Average Spending Per Category: {response['avg_category']}
        - Spending Per Vendor: {response['spending_per_vendor']}
        - Percentage Change in Vendor Spending (from previous): {response['pct_change_vendor']}
        - Average Spending Per Vendor: {response['avg_vendor']}
        - Top 5 Vendors: {response['top_5_vendors']}

        Write a polished executive summary that highlights:
        - Where spending increased or decreased (mention categories/vendors with the highest percentage changes)
        - Top spending categories and vendors
        - Average behavior and how it compares to past trends
        - Any unusual or notable trends worth management attention

        Your output should be a short, digestible paragraph — **only return the final text to paste into a report or email**. No explanations, no formatting, just the summary itself.
        """
        ai_resp = client.models.generate_content(
            model='gemini-2.0-flash-001', contents=prompt
        )
        ai_fi_summary = ai_resp.text
    except Exception as e:
        ai_fi_summary = 'Error generating financial summary with AI.' 
        print(e)
    finally:
        response['fi_summary'] = ai_fi_summary

    # Inserting into our Snowflake Schema 
    details = {k:v for k,v in response.items() if k not in ['begin_date', 'end_date', 'fi_summary']}
    check_report_exists(response['begin_date'], response['end_date'], details, response['fi_summary'])
    return response

details = {
  "total_spent": 41626.36,
  "total_transactions": 150,
  "unique_categories": [
    "dining",
    "entertainment",
    "groceries",
    "healthcare",
    "shopping",
    "transportation",
    "utilities"
  ],
  "unique_vendors": [
    "Amazon",
    "Apple",
    "CVS",
    "Costco",
    "Lyft",
    "Netflix",
    "Starbucks",
    "Target",
    "Uber",
    "Walmart"
  ],
  "spending_per_category": {
    "healthcare": 2987.99,
    "groceries": 5016.26,
    "transportation": 5245.48,
    "dining": 5866.95,
    "utilities": 7004.15,
    "entertainment": 7535.22,
    "shopping": 7970.31
  },
  "pct_change_category": {
    "groceries": 0.01,
    "healthcare": 0.01,
    "entertainment": 0.01,
    "utilities": 0.01,
    "dining": 0.01,
    "shopping": 0.01,
    "transportation": 0.01
  },
  "avg_category": {
    "groceries": 2656.77,
    "healthcare": 2511.74,
    "entertainment": 2334.72,
    "utilities": 1955.65,
    "dining": 1748.49,
    "shopping": 1672.09,
    "transportation": 996.0
  },
  "spending_per_vendor": {
    "Target": 150.3,
    "Uber": 923.35,
    "Lyft": 1598.87,
    "Walmart": 3305.34,
    "CVS": 3316.66,
    "Amazon": 3413.65,
    "Netflix": 6063.13,
    "Costco": 6093.96,
    "Starbucks": 8138.76,
    "Apple": 8622.34
  },
  "pct_change_vendor": {
    "Starbucks": 0.01,
    "Target": 0.01,
    "CVS": 0.01,
    "Walmart": 0.01,
    "Uber": 0.01,
    "Apple": 0.01,
    "Costco": 0.01,
    "Lyft": 0.01,
    "Amazon": 0.01,
    "Netflix": 0.01
  },
  "avg_vendor": {
    "Starbucks": 2874.11,
    "Target": 2712.92,
    "CVS": 2031.32,
    "Walmart": 2021.04,
    "Uber": 1137.88,
    "Apple": 1105.55,
    "Costco": 1101.78,
    "Lyft": 532.96,
    "Amazon": 307.78,
    "Netflix": 50.1
  },
  "top_5_vendors": {
    "Target": 150.3,
    "Uber": 923.35,
    "Lyft": 1598.87,
    "Walmart": 3305.34,
    "CVS": 3316.66
  },
}

begin_date = "2024-07-19"
end_date = "2025-07-01"
fi_summary = "From 2024-07-19 to 2025-07-01, total spending amounted to $41,626.36 across 150 transactions. While all spending categories and vendors experienced a nominal 1% increase compared to the previous period, some areas require attention. \"Shopping\" ($7,970.31), \"Entertainment\" ($7,535.22), and \"Utilities\" ($7,004.15) represent the highest spending categories, suggesting potential areas for cost optimization. Vendor spending is heavily concentrated with \"Apple\" ($8,622.34) and \"Starbucks\" ($8,138.76) leading expenditures. The high concentration of spending within a few categories and vendors warrants further investigation to determine if strategic sourcing or negotiation opportunities exist."
# check_report_exists(begin_date, end_date, details, fi_summary)

transform_summary()
