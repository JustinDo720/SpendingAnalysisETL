import extractor as ex
from typing import Dict, Any
from collections import defaultdict
from datetime import datetime 
import pandas as pd
from google import genai
import os
import dotenv

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
    category_pct_change = category_df.pct_change().fillna(0).iloc[-1].to_dict()
    vendor_pct_change = vendor_df.pct_change().fillna(0).iloc[-1].to_dict()

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

    return response

resp = transform_summary()
print(resp)
