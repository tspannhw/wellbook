#!/usr/bin/env ./local-py/bin/python

#LAS standard spec:
#https://esd.halliburton.com/support/LSM/GGT/ProMAXSuite/ProMAX/5000/5000_8/Help/promax/las_overview.pdf

import sys, json

def process_field(line_type, line, fields):
  if line[0] == '.': line = line[1:] #remove leading '.'s
  
  if line_type == 'O':
    if 'comments' not in fields: fields['comments'] = line
    else: fields['comments'] += line
    return

  #parse field metadata
  name = line.split('.')[0].strip()
  UOM = line.split('.')[1].split(' ')[0].strip()
  if UOM == '': val = ' '.join(line.split('.')[1:]).strip().split(':')[0].strip()
  else: val = ' '.join(line.split(UOM)[1:]).strip().split(':')[0].strip()

  desc = line.split(':')[1].strip().replace('\t', ' ').replace('  ', ' ')
  field = {}
  if val != '': field['val'] = val
  if UOM != '': field['UOM'] = UOM
  if desc != '': field['desc'] = desc
  field['block'] = line_type
  fields[name] = field
  
  if line_type == 'C': #keep track of curve information block field order
    if 'cib_order' in fields: fields['cib_order'].append(name)
    else: fields['cib_order'] = [name]

def process_depth_entry(line, fields):
  depth = {}
  #for each reading at this depth, look up sensor name corresponding to the value
  for idx, reading in enumerate(line.split()): depth[fields['cib_order'][idx]] = reading
  return depth

def emit(file_no, fields, rec):
  sys.stdout.write('%s\n' % (file_no + '\t' + fields.lower() + '\t' + json.dumps(rec).lower()))

def process_rec(key, rec):
  file_no = key.split('/')[1].split('-')[0]
  sys.stderr.write('Processing rec:\n%s' % ('\n'.join(rec.split('\n')[:25])))
  fields = {}
  for line in filter(lambda x: len(x) > 0 and x[0] != '#', rec.split('\n')): #filter out blank lines
    if line[0] == '~':
      line_type = line[1]
    else: #All lines but ~A deal with metadata
      sys.stderr.write('Handling: ' + line)
      if line_type != 'A': process_field(line_type, line, fields)
      else: emit(file_no, json.dumps(fields), process_depth_entry(line, fields))
        

rec = ''
key = ''
for line in sys.stdin:
  if '__key' in line:
    if rec != '': process_rec(key, rec) #just finished reading data for a file
    key = line.split('__key')[0]
    sys.stderr.write('Starting read of File: ' + key + '\n')
  if '__data' in line: #just starting to read data for a file
    rec = line.split('__data')[1]
  else: rec += line #continuing to read data for a file

if rec != '': process_rec(key, rec)