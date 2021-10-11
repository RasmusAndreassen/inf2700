from math import ceil
from os import remove
from time import sleep
from subprocess import PIPE, run
from sys import argv, exit, stderr
from random import randint, choices, sample
from string import ascii_lowercase
from typing import Generator, Iterable

SUPPRESS=False
BENCHMARK=False

ids:list[int] = []

test_attrs = ['id','name']

class Field:
  def __init__(self, name:str, type:str, max:int=None, incr:bool=False, uniq:bool=False, len:int=None):
    self.name = name
    self.type = type
    self.incr = incr
    self.uniq = uniq
    if type == 'int':
      assert((max is not None) or incr), 'field of type int needs maximum specifier'
      self.max = max
      self.len = 4
    elif type == 'str':
      assert(len is not None), 'field of type str needs length specifier'
      self.len = len
    else:
      raise ValueError('Field constructor got invalid type; "{}"'.format(type))

  def __str__(self):
    match self.type:
      case 'int':
        t_str = 'int'
      case 'str':
        t_str = 'str[{}]'.format(self.len)
    return ' '.join((self.name, t_str))

  def random(self, max:int=None, val:int|Iterable=None):
    '''Field.random()->Field.type
    Returns a randomised value based on Field.type
    max
    - int: max value (must be set if not inc is set)
    - str: max length (defaults to Field.len)
    If inc is set, the value will be incremented
    with a geometrically distributed amount,
    with probability p =0.5. In this case both val and inc 
    must be set.
    val is a representation of previous values,
    that is required by the uniq and inc modes. 
    '''
    match self.type:
      case 'int':
        if self.incr:
          if self.type != 'int':
            raise TypeError('Only fields of type `int` can be incremented, field `{}` is type `{}`'.format(self.name, self.type))
          if not isinstance(val, int):
            raise TypeError('val must be integer when incrementing')
          while(randint(0,1)):
            val+=1
          value = val
        else:
          if max is None:
            max = self.max
          value = randint(1,max)
          if self.uniq:
            if not isinstance(val, Iterable):
              raise TypeError('val must be iterable when rendom should be unique')
            while value in val:
              value = randint(1,max)
      case 'str':
        upper_bound = max if max is not None else self.len
        n = randint(1,upper_bound)
        value = ''.join(choices(ascii_lowercase, k=n))

    return value


test_field_names = ['id','name']
test_fields = {
  'id':{
    'type':'int',
    'uniq':True,
    'max':1024*1024*1024,
    },
  'name':{
    'type':'str',
    'len':16,
  },
  'val':{
    'type':'int',
    'incr':True
  },
}

class Schema:
  def __init__(self, fields:dict[str,dict[str]]) -> None:
      self.fields:list[Field] = []
      self.first:dict[str,] = {}
      for name, field in fields.items():
        if 'init' in field:
          self.first[name] = field['init']
          del field['init']
        self.fields.append(Field(name, **field))
      self.history:list[dict] = []

      self.len = sum(field.len for field in self.fields)
      self.n_records = 0

  def last(self):
    return self.history[len(self.history)-1] if len(self.history) > 0 else None

  def all_prev(self, f_name:str):
    return [record[f_name] for record in self.history]

  def next_record(self) -> list:
    new = {}

    for field in self.fields:

      if field.incr:
        try:
          val = self.last()['val']
        except TypeError:
          try:
            val = self.first[field.name]
          except KeyError:
            raise AssertionError('An incremented field must have an initial value')

      elif field.uniq:
        val = self.all_prev(field.name)

      else:
        val = None

      val = field.random(val=val)

      new[field.name] = val

    self.history.append(new)
    self.n_records += 1
    
    return list(self.last().values())

  def __str__(self):
    return '('+','.join(str(field) for field in self.fields)+')'

def gen_genTable_file(n:int, s:Schema, fname:str, tname:str):
  # open .sql file
  f = open(fname, "w")

  # make the table
  f.write('create table {} {};\n'.format(tname, s))

  # write insert statements
  for i in range(n):
    r = s.next_record()
    values = ','.join(str(value) for value in r)
    istr = 'insert into {} values ({});\n'.format(tname, values)
    #print(s)
    f.write(istr)
  f.write('quit')

def gen_table(n:int, s:Schema, fname:str, tname:str):
  gen_genTable_file(n,s,fname,tname)
  res = run(['./run_front', '-n', '-c{}'.format(fname)], text=True, stdout=PIPE, )
  print(res.stdout)
  # res = run(['./run_front', '-n', ], text=True, input='show database', stdout=PIPE, stderr=PIPE)
  # print(res.stderr)


tests = {}

BLOCK_SIZE = 512
PAGE_HEADER_SIZE = 20

def print_tests():
  for test, q in tests.items():
    print(test)
    if isinstance(q, str):
      print(q)
    else:
      for q in q:
        print('-', q)

def gen_queries(n:int, s:Schema, table_name:str):
  search_value = randint(0,2048+n)
  
  # compute key values
  recs_per_page = (BLOCK_SIZE-PAGE_HEADER_SIZE)// s.len
  n_pages = ceil(s.n_records/recs_per_page)

  select = 'select * from {} where val = {};\nquit'

  search_value = s.history[0]['val']
  tests["== Select first value"] = (
    search_value,
    select.format(table_name, search_value)
  )

  search_value = s.last()['val']
  tests["== Select last value"] = (
    search_value,
    select.format(table_name, search_value)
  )

  search_value = s.history[recs_per_page-1]['val']
  tests["== Select last value of first page"]=(
    search_value,
    select.format(table_name, search_value)
  )

  search_value = s.history[(n_pages-1)*recs_per_page]['val']
  tests["== Select first value of last page"] = (
    search_value,
    select.format(table_name, search_value)
  )

  pagens = []
  for i in range(nrand):
    n = randint(1,n_pages-2)
    while n in pagens:
      n = randint(1,n_pages-2)
    pagens.append(n)

  pagens.sort()
  search_values = (s.history[p_index*recs_per_page]['val'] for p_index in pagens)
  tests["== Select first value of {} random pages".format(nrand)] = tuple(
    (
      search_value,
      select.format(table_name, search_value),
    ) for search_value in search_values
  )

  pagens = []
  for i in range(nrand):
    n = randint(1,n_pages-2)
    while n in pagens:
      n = randint(1,n_pages-2)
    pagens.append(n)  

  pagens.sort()
  search_values = [s.history[(p_index+1)*recs_per_page-1]['val'] for p_index in pagens]
  tests["== Select last value of {} random pages".format(nrand)] = tuple(
    (
      search_value,
      select.format(table_name, search_value),
    ) for search_value in search_values
  )

  indeces = []
  for i in range(nrand):
    n = randint(0,s.n_records-1)
    while n in indeces:
      randint(0,s.n_records-1)
    indeces.append(n)
  indeces.sort()
  search_values = [s.history[index]['val'] for index in indeces]
  tests["== Select {} random present values".format(nrand)] = tuple(
    (
      search_value,
      select.format(table_name, search_value)
    ) for search_value in search_values
  )

  present = s.all_prev('val')
  search_values = []
  smallest = s.history[0]['val']
  largest = s.last()['val']

  for i in range(nrand):
    value = randint(smallest,largest)
    while value in present:
      value = randint(s.history[0]['val'],s.last()['val'])
    search_values.append(value)

  search_values.sort()
  tests["== Select {} random non-present values".format(nrand)] = tuple(
    (
      search_value,
      select.format(table_name, search_value)
    ) for search_value in search_values
  )

total = 0
successes = 0

def run_and_compare(query:str):
  bres = run(['./run_front', '-n', '-by'], text=True, input=query, stderr=PIPE).stderr
  lres = run(['./run_front', '-n', ], text=True, input=query, stderr=PIPE).stderr
  global total
  global successes
  total+=1
  if bres == lres:
    if not SUPPRESS: print('\tpassed\n')
    successes +=1
  else:
    if not SUPPRESS: print('\tfailed')
    if not SUPPRESS: print('expected:', lres, sep='\n')
    if not SUPPRESS: print('got:', bres, sep='\n')
    if not SUPPRESS: run(['gdb', '--args', 'run_front', '-n', '-by'], text=True, stderr=PIPE)

def run_queries():
  for test, q in tests.items():
    if not SUPPRESS: print(test)
    if isinstance(q[0], int):
      if not SUPPRESS: print('Searching for value:', q[0])
      run_and_compare(q[1])
    else:
      for q in q:
        if not SUPPRESS: print('Searching for value:', q[0])
        run_and_compare(q[1])
    if not SUPPRESS: print('=======================')

  s = "well done!" if total == successes else "too bad..."
  if not SUPPRESS: print("Passed {}/{} tests,".format(successes,total), s)

def cleanup(fname, tname):
  remove(fname)
  run(['./run_front', '-n'], text=True, input="drop table {};\nquit".format(tname), )
  pass

def superrandom():
  successes = 0
  for i in range(1):
    N = randint(100,10000)
    n = randint(int(N*0.0125),int(N*0.0375))
    print('Running tests with N={},n={}'.format(N,n))
    res:int = run(['python3.10', __file__, str(N), str(n)], text=True, stdout=PIPE).returncode
    if res == 0:
      print('passed')
      successes+=1
    
  print('Succeeded {} times out of 1'.format(successes))
  exit(0)

if __name__ == '__main__':
  # get number of entries to create from argv
  i = 1
  while argv[i].startswith('--'):
    match argv[i][2:]:
      case 'suppress':
        SUPPRESS = True
      case 'benchmark':
        BENCHMARK = True
      case 'superrandom':
        superrandom()
    i+=1
  try:
    n:int = int(argv[i])
  except IndexError:
    n = 1024
  except ValueError:
    print(argv[i],'is not an integer')
    exit(-1)

  if n <= 0:
    print('n must be greater than 0, got', n)
    exit(-1)
  
  try:
    nrand = int(argv[i+1])
  except IndexError:
    pass

  # generate table schema to create
   # select a random set of field names
  chosen_attrs = sample(test_field_names, randint(0,len(test_field_names)))
  index = randint(0,len(chosen_attrs))
  chosen_attrs.insert(index, 'val') # table should always have field val
   # generate a mapping of all the chosen field names with their corresponding arguments
  attributes = dict(item for item in test_fields.items() if item[0] in chosen_attrs)
  attributes['val']['init'] = randint(0,n)
  s = Schema(attributes)

  fname = 'generate_table.sql'
  tname = 'IntField'
  gen_table(n, s, fname, tname)
  

  gen_queries(n, s, tname)

  run_queries()
  #sleep(5)

  cleanup(fname, tname)

  if successes == total:
    exit(0)
  else:
    exit(1)
  #res = run(['./run_front', '-n', '-by',], text=True, input='help', stdout=PIPE)
  #print_tests()
  #print(res.stdout)
