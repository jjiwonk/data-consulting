import sys
sys.path.append('/home/ec2-user/data-consulting')

import batch_test
import setting.ec2_directory as dr
import spreadsheet.ec2_spreadsheet as spreadsheet

doc = spreadsheet.spread_document_read('https://docs.google.com/spreadsheets/d/14i42NrpnA_9k8nCgyc4Y5KssLP8tOOXUyzYw0un7qXY/edit#gid=211027705')
ADVERTISER = '무신사'
result_dir = dr.ec2_dir

if __name__ == "__main__":
    try:
        print('start')
        batch_test.landing_check_solution_exec(doc, ADVERTISER)
    except Exception as e:
        f = open(dr.ec2_dir + '/error_log.txt', 'w')
        f.write(f'error with {e}.')
        f.close()
