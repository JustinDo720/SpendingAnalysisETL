from fastapi import FastAPI, HTTPException
from pydantic import BaseModel 
from loader import get_snowflake_connection
from typing import Dict, Any
import json 

# Initialize our FASTAPI application 
app = FastAPI()

# Creating a Data Validation Layer with Pydantic 
class DateRange(BaseModel):
    begin_date: str 
    end_date: str 

@app.get('/dates')
def get_date_ranges():
    """
       Date Ranges in our Snowflake Datawarehouse for financial reports 
    """
    try: 
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Query to grab all date ranges 
        date_query = """
            SELECT begin_date, end_date 
            FROM file_details
        """

        cursor.execute(date_query)
        data = cursor.fetchall()

        return {
            'dates': [
                {'begin_date': date[0], 'end_date': date[1]}
                for date in data
            ]
        }
    except Exception as e:
        return {
            'msg': 'Error with date ranges'
        }
    finally:
        conn.close() 
        cursor.close() 

@app.post('/dates/summary')
def date_range_summary(date_range: DateRange) -> Dict[str, Any]:
    """
        Be sure the format for our date range is in YYYY-MM-DD
    """
    try:
        conn = get_snowflake_connection() 
        cursor = conn.cursor()

        summary_query = """
            SELECT *
            FROM file_details
            WHERE begin_date = %s and end_date = %s
        """

        cursor.execute(summary_query, (date_range.begin_date, date_range.end_date))
        financial_details = cursor.fetchone()

        if financial_details:
            return {
                'begin_date': financial_details[1],
                'end_date': financial_details[2],
                'details': json.loads(financial_details[3]),
                'fi_summary': financial_details[4],
                'created': financial_details[5]
            }
        else:
            return {
                'msg': 'No financial details found with these date range...'
            }

    except Exception as e:
        return {
            'err': str(e)
        }
    finally:
        conn.close() 
        cursor.close()