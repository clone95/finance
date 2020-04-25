from datetime import datetime

def get_quarter(date):
    
    date = datetime.strptime(date, '%Y-%m-%d')

    quarter_dictionary = {
        "q1" : [1,2,3],
        "q2" : [4,5,6],
        "q3" : [7,8,9],
        "q4" : [10,11,12]
    }

    month = date.month
    quarter = [key for (key, value) in quarter_dictionary.items() if month in value][0]
    quarter = str(date.year) + '-' + quarter

    return quarter 


def get_newest_date(dates: list):

    dates = [datetime.strptime(date, '%Y-%m-%d') for date in dates]
    newest = max(dates)
    
    return newest
