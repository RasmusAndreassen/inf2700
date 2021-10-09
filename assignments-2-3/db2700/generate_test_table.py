from os import remove, system, wait
from sys import argv, exit
from random import randint, choices, sample

english_alphabet = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','x']

ids:list[int] = []

test_attrs = ['id','name']
types = {'id':'int', 'name':'str[16]', 'val':'int'}

class Record:
  def __init__(self, attributes:list[str]) -> None:
      self.values = {'val':randint(0,1024)}
      self.attrs = attributes
      for attr in self.attrs:
        self.update_attr(attr)
  
  def update_attr(self, attr:str):
    match attr:
      case 'id':
        self.rand_id()
      case 'name':
        self.rand_name()
      case 'val':
        self.next_val()

  def rand_id(self):
    id = randint(0,1024*1024*1024)
    while (id in ids):
      id = randint(0,1024*1024*1024)

    self.values['id'] = id

  def rand_name(self):
    chars = choices(english_alphabet, k=randint(2,16))
    self.values['name'] = ''.join(chars)

  def next_val(self):
    val = self.values['val'] if not None else 0
    while(randint(0,1)):
      val+=1
    self.values['val'] = val
    

  def next(self):
    old = []

    for attr in self.attrs:
      old.append(self.values[attr])
      self.update_attr(attr)
    
    return old

def gen_q_file(n, fname):
  # open .sql file
  f = open(fname, "w")
  table_name = 'IntField'

  # make the table
  f.write('create table {} ('.format(table_name)+ ','.join('{} {}'.format(a, types[a]) for a in attributes) +');\n')

  # write insert statements
  for i in range(n):
    astr = ','.join(str(value) for value in r.next())
    s = 'insert into {} values ('.format(table_name)+astr+');\n'
    #print(s)
    f.write(s)

  search_value = randint(0,2048+n)
  # test binary search
  f.write('select * from {} where val = {};\n'.format(table_name, search_value))
  # delete table
  f.write('drop table {};\n'.format(table_name))
  f.close()

  return search_value

if __name__ == '__main__':
  # get number of entries to create from argv
  try:
    n:int = int(argv[1])
  except IndexError:
    n = 1024
  except ValueError:
    print(argv[1],'is not an integer')
    exit(-1)
  if n <= 0:
    print('n must be greater than 0, got', n)
    exit(-1)


  # generate table schema to create
  attributes = sample(test_attrs, randint(1,2))
  attributes.insert(randint(1,len(attributes)), 'val') # table must have field val
  r = Record(attributes)

  fname = 'generate_and_test.sql'
  val = gen_q_file(n, fname)

  print ('Searching for value {}'.format(val))

  wait(system('./run_front -c {}\n'.format(fname)))

