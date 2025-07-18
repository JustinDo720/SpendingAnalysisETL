import extractor as ex
from typing import Dict, Any
from collections import defaultdict
from datetime import datetime 

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

    for file in all_uploaded_files:
        summary = ex.get_summary(file)
        if not summary:
            continue 

        # Tracking Date 
        begin_date_datetime_format = datetime.strptime(file.get('begin_date', None), date_format).date()
        if begin_date is None:
            begin_date = begin_date_datetime_format
        else:
            # Compare current begin_date 
            begin_date = min(begin_date, begin_date_datetime_format)

        end_date_datetime_format = datetime.strptime(file.get('end_date', None), date_format).date()
        if end_date is None:
            end_date = end_date_datetime_format
        else:
            # Compare current begin_date 
            end_date = max(end_date, end_date_datetime_format)

        total_expenses += float(summary.get('total_spent', 0))
        total_transactions += summary.get('total_transactions', 0)

        # Looping through category 
        for category, amount in summary['spending_per_category'].items():
            category_total[category] += round(float(amount),2)
            category_set.add(category)

        # Looping through vendor 
        for vendor, amount in summary['spending_per_vendor'].items():
            vendor_total[vendor] += round(float(amount),2)
            vendor_set.add(vendor)
        
    sorted_category_total = sorted(category_total, key=lambda cat: cat[1])
    sorted_vendor_total = sorted(vendor_total, key=lambda vendor: vendor[1])

    return {
        'total_spent': round(total_expenses, 2),
        'total_transactions': total_transactions,
        'unique_categories': sorted(list(category_set)),
        'unique_vendors': sorted(list(vendor_set)),
        'spending_per_category': dict(sorted_category_total),
        'spending_per_vendor': dict(sorted_vendor_total),
        'top_5_vendors': dict(sorted_vendor_total[:5]),
        'begin_date': begin_date.isoformat(),
        'end_date': end_date.isoformat()
    }