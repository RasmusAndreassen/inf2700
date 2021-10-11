from math import ceil, sqrt
from os import remove
from time import sleep
from subprocess import PIPE, TimeoutExpired, run
from sys import argv, exit, stderr
from random import randint, choices, sample
from string import ascii_lowercase
from typing import Generator, Iterable

from cfg import BFNAME, FNAMES, GFNAME, LFNAME, QFNAME, T_NAME

SUPPRESS=False
BENCHMARK=False
DEBUG=False


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
        len = randint(1,upper_bound)
        value = ''.join(choices(ascii_lowercase, k=len))

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
      self.f_history:dict[str,list] = {}

      self.len = sum(field.len for field in self.fields)
      self.n_records = 0

  def last(self):
    return self.history[len(self.history)-1] if len(self.history) > 0 else None

  def all_prev(self, f_name:str):
    try:
      return self.f_history[f_name]
    except KeyError:
      return []
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
      try:
        self.f_history[field.name].append(val)
      except KeyError:
        self.f_history[field.name] = [val]

    self.history.append(new)
    self.n_records += 1
    
    return list(self.last().values())

  def __str__(self):
    return '('+','.join(str(field) for field in self.fields)+')'

def gen_genTable_file(N:int, s:Schema):
  # open .sql file
  f = open(GFNAME, "w")

  # make the table
  f.write('create table {} {};\n'.format(T_NAME, s))

  # write insert statements
  for i in range(N):
    r = s.next_record()
    values = ','.join(str(value) for value in r)
    istr = 'insert into {} values ({});\n'.format(T_NAME, values)
    #print(s)
    f.write(istr)
  f.write('quit')

def gen_table(N:int, s:Schema):
  gen_genTable_file(N,s)
  res = run(['./run_front', '-n', '-c{}'.format(GFNAME)], text=True, stdout=PIPE, )
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

def gen_queries(N:int, np:int, n:int, s:Schema):
  search_value = randint(0,2048+N)
  
  # compute key values
  recs_per_page = (BLOCK_SIZE-PAGE_HEADER_SIZE)// s.len
  n_pages = ceil(s.n_records/recs_per_page)

  select = 'select * from {} where val = {};\nquit'

  search_value = s.history[0]['val']
  tests["== Select first value"] = (
    search_value,
    select.format(T_NAME, search_value)
  )

  search_value = s.last()['val']
  tests["== Select last value"] = (
    search_value,
    select.format(T_NAME, search_value)
  )

  search_value = s.history[recs_per_page-1]['val']
  tests["== Select last value of first page"]=(
    search_value,
    select.format(T_NAME, search_value)
  )

  search_value = s.history[(n_pages-1)*recs_per_page]['val']
  tests["== Select first value of last page"] = (
    search_value,
    select.format(T_NAME, search_value)
  )

  pagenums = []
  for i in range(np):
    pagenum = randint(1,n_pages-2)
    while pagenum in pagenums:
      pagenum = randint(1,n_pages-2)
    pagenums.append(pagenum)

  pagenums.sort()
  search_values = (s.history[p_index*recs_per_page]['val'] for p_index in pagenums)
  tests["== Select first value of {} random pages".format(np)] = tuple(
    (
      search_value,
      select.format(T_NAME, search_value),
    ) for search_value in search_values
  )

  pagenums = []
  for i in range(np):
    pagenum = randint(1,n_pages-2)
    while pagenum in pagenums:
      pagenum = randint(1,n_pages-2)
    pagenums.append(pagenum)  
  pagenums.sort()

  search_values = [s.history[(p_index+1)*recs_per_page-1]['val'] for p_index in pagenums]
  tests["== Select last value of {} random pages".format(np)] = tuple(
    (
      search_value,
      select.format(T_NAME, search_value),
    ) for search_value in search_values
  )

  indeces = []
  print(s.n_records-1)
  for i in range(n):
    index = randint(0,s.n_records-1)
    while index in indeces:
      index = randint(0,s.n_records-1)
    indeces.append(index)
  indeces.sort()

  search_values = [s.history[index]['val'] for index in indeces]
  tests["== Select {} random present values".format(n)] = tuple(
    (
      search_value,
      select.format(T_NAME, search_value)
    ) for search_value in search_values
  )

  present = s.all_prev('val')
  search_values = []
  smallest = s.history[0]['val']
  largest = s.last()['val']

  for i in range(n):
    value = randint(smallest,largest)
    while value in present:
      value = randint(s.history[0]['val'],s.last()['val'])
    search_values.append(value)
  search_values.sort()

  tests["== Select {} random non-present values".format(n)] = tuple(
    (
      search_value,
      select.format(T_NAME, search_value)
    ) for search_value in search_values
  )
def gen_bm_queries(N:int, s:Schema):
  search_value = randint(0,2048+N)
  with open(QFNAME, "w") as f:
    # compute key values
    recs_per_page = (BLOCK_SIZE-PAGE_HEADER_SIZE)// s.len
    n_pages = ceil(s.n_records/recs_per_page)

    slct = 'select * from {} where val = {};\n'

    # very first
    search_value = s.history[0]['val']
    f.write(slct.format(T_NAME, search_value))

    # very last
    search_value = s.last()['val']
    f.write(slct.format(T_NAME, search_value))

    # last of first page
    search_value = s.history[recs_per_page-1]['val']
    f.write(slct.format(T_NAME, search_value))

    # first of last page
    search_value = s.history[(n_pages-1)*recs_per_page]['val']
    f.write(slct.format(T_NAME, search_value))

    pagenums = []
    for i in range(n):
      pagenum = randint(1,n_pages-2)
      while pagenum in pagenums:
        pagenum = randint(1,n_pages-2)
      pagenums.append(pagenum)
    pagenums.sort()

    # first of random pages
    search_values = [s.history[p_index*recs_per_page]['val'] for p_index in pagenums]
    for search_value in search_values:
      f.write(slct.format(T_NAME, search_value))

    pagenums = []
    for i in range(n):
      pagenum = randint(1,n_pages-2)
      while pagenum in pagenums:
        pagenum = randint(1,n_pages-2)
      pagenums.append(pagenum)  
    pagenums.sort()

    # last of random pages 
    search_values = [s.history[(p_index+1)*recs_per_page-1]['val'] for p_index in pagenums]
    for search_value in search_values:
      f.write(slct.format(T_NAME, search_value))
      
    

    indeces = []
    for i in range(n):
      index = randint(0,s.n_records-1)
      while index in indeces:
        index = randint(0,s.n_records-1)
      indeces.append(index)
    indeces.sort()

    # completely random present
    search_values = [s.history[index]['val'] for index in indeces]
    for search_value in search_values:
      f.write(slct.format(T_NAME, search_value))

    # reference values
    smallest = s.history[0]['val']
    largest = s.last()['val']
    present = s.all_prev('val')

    search_values = []
    for i in range(n):
      value = randint(smallest,largest)
      while value in present:
        value = randint(s.history[0]['val'],s.last()['val'])
      search_values.append(value)
    search_values.sort()

    # nonpresent
    for search_value in search_values:
      f.write(slct.format(T_NAME, search_value))

    f.write('show pager\n')
    f.write('quit')
    

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
    if DEBUG: run(['gdb', '--args', 'run_front', '-n', '-by'], text=True, stderr=PIPE)

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

def run_benchmark():
  with open(BFNAME, 'w+') as bout:
    with open(LFNAME, 'w+') as lout:
      try:
        bout.write(run(['./run_front', '-n', '-by', '-c{}'.format(QFNAME)], text=True, stderr=PIPE, ).stderr)
        lout.write(run(['./run_front', '-n', '-c{}'.format(QFNAME)], text=True, stderr=PIPE, ).stderr)
      except UnicodeDecodeError:
        print('error decoding')
        cleanup()
        exit(1)
      bout.seek(0)
      lout.seek(0)
      
      bres = bout.readlines().pop().split().pop().split('/')
      lres = lout.readlines().pop().split().pop().split('/')

      print('=======================================')
      print('\tseeks\treads\twrites\tIO')
      print('binary\t{}'.format(',\t'.join(bres)))
      print('linear\t{}'.format(',\t'.join(lres)))
      print('=======================================')

def cleanup():
  for fname in FNAMES:
    try:
      remove(fname)
    except FileNotFoundError:
      continue
  run(['./run_front', '-n'], text=True, input="drop table {};\nquit".format(T_NAME), )
  pass

def superrandom(times):
  successes = 0
  for i in range(times):
    N = randint(100,100000)

    max = ceil(0.5*sqrt(N))
    min = ceil(0.01*sqrt(N))
    np = randint(min,max)

    max = int(0.1*N)
    min = int(0.001*N)
    n = randint(min,max)
    print('Running tests with N={},np={},n={}'.format(N,np,n))

    res:int = run(['python3.10', __file__, str(N), str(np), str(n)], text=True, stdout=PIPE).returncode
    if res == 0:
      print('passed')
      cleanup()
      successes+=1
    else:
      print('failed')
      cleanup()
    # except TimeoutExpired:
    #   print('timed out')
    #   times-=1
    #   cleanup()
    #   continue

  cleanup()
  print('Succeeded {} times out of {}'.format(successes, times))
  exit(0)

def print_info():
  print('Usage: {{python}} {} [flags] [N [np [n]]]'.format(argv[0]))
  print('   or: {{python}} {} --superrandom [times]'.format(argv[0]))
  print('Runs randomised queries on a randomised table of N entries (default to 1024).')
  print('  np denotes the number of random pages (default 5) to test')
  print('  n denotes the number of random values (default 10) to search from in the table. Will search for n present and n nonpresent values.')
  print(' flags:')
  print('   --help\t\tprint this and exit')
  print('   --benchmark\t\trun benchmark of both linear and binary search, results are printed to stdout')
  print('   --superrandom\truns this program `times` times (default to 5), counting the number of complete successes')
  print('                \tNOTE: superrandom mode is incredibly slow')
  print('   --suppress\t\trun without printing results')
  exit(0)
if __name__ == '__main__':
  try:
    # get number of entries to create from argv
    i = 1
    try:
      while argv[i].startswith('--'):
        match argv[i][2:]:
          case 'help':
            print_info()
          case 'suppress':
            SUPPRESS = True
          case 'benchmark':
            BENCHMARK = True
          case 'debug':
            DEBUG=True
          case 'superrandom':
            try:
              superrandom(int(argv[i+1]))
            except IndexError:
              print('`times` omitted, defaulting to 5')
              superrandom(5)
        i+=1
    except IndexError:
      pass
    try:
      N:int = int(argv[i])
    except IndexError:
      print('`N` omitted, defaulting to 1024')
      N = 1024
    except ValueError:
      print(argv[i],'is not an integer')
      exit(-1)
    finally:
      i+=1

    if N <= 0:
      print('N must be greater than 0, got', N)
      exit(-1)
    
    try:
      np = int(argv[i])
    except IndexError:
      print('`np` omitted, defaulting to 3')
      np = 3
    except ValueError:
      print(argv[i],'is not an integer')
      exit(-1)
    finally:
      i+=1


    try:
      n = int(argv[i])
    except IndexError:
      print('`n` omitted, defaulting to 10')
      n = 10
    except ValueError:
      print(argv[i],'is not an integer')
      exit(-1)
    

    # generate table schema to create
    # select a random set of field names
    chosen_attrs = sample(test_field_names, randint(0,len(test_field_names)))
    index = randint(0,len(chosen_attrs))
    chosen_attrs.insert(index, 'val') # table should always have field val
    # generate a mapping of all the chosen field names with their corresponding arguments
    attributes = dict(item for item in test_fields.items() if item[0] in chosen_attrs)
    attributes['val']['init'] = randint(0,N)
    s = Schema(attributes)


    try:
      if T_NAME in run(['./run_front', '-n'], text=True, input="show database\nquit", stderr=PIPE).stderr:
        run(['./run_front', '-n'], input="drop table {}\nquit".format(T_NAME).encode('utf-8'), stderr=PIPE)
    except UnicodeDecodeError:
      print('error decoding')
      cleanup()
      exit(1)
    

    gen_table(N, s)

    if BENCHMARK:
      gen_bm_queries(N, s)

      run_benchmark()
    else:
      gen_queries(N, np, n, s)

      run_queries()
    #sleep(5)

    cleanup()

    if successes == total:
      exit(0)
    else:
      exit(1)
    #res = run(['./run_front', '-n', '-by',], text=True, input='help', stdout=PIPE)
    #print_tests()
    #print(res.stdout)
  finally:
    cleanup()