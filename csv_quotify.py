#!/usr/bin/env/python

inp = open('top-1m.csv', 'r')
out = open('top-1m__.csv', 'a+')
out.write('"domain_id,"domain_name"')
id = ''
domain = ''

while True:
    line = inp.readline()
    
    if len(line) > 0:
        cPos = line.find(',')
        id = line[:cPos]
        domain = line[cPos+1:]
        domain = '"'+domain.rstrip()+'"'
        
        final = id+','+domain+'\n'
        out.write(final)
    else:
        break

inp.close()
out.close()