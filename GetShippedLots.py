#!/usr/bin/python
# Script to export the Shipped lots to end customer for performing Archiving of these lots
# by IKA
# packages required: python-suds, python-pkg-resources

from suds.client import Client
import sys

if not (len(sys.argv)-1) == 2:
    print 'You did not provide arguments for StartDate and EndDate'
    sys.exit(1) 

startDate = sys.argv[1]
endDate   = sys.argv[2]

client = Client('@VISUALSERVER@')
results = client.service.findLotsShipped(startDate, endDate)

for list in results.item:
    print list['lotNumber']
    
