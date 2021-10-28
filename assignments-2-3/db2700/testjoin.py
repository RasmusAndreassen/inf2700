from sys import stdin

Me = 'Me'
You = 'You'

curr:dict         = None
me:dict[int,int]  = {}
you:dict[int,int] = {}
join:dict[int,dict[str,dict[int,bool]]] = None

def natural_join(me:dict[int,int], you:dict[int,int]):
  join:dict[int,dict[str,list[int]]] = {}
  for Id, val in me.items():
    try:
      join[val][Me][Id] = False
    except KeyError as e:
      match e.args[0]:
        case key if key is Me:
          join[val][Me] = {Id: False}
        case key if key is val:
          join[val] = {Me:{Id: False}}
  for Id, val in you.items():
    try:
      join[val][You][Id] = False
    except KeyError as e:
      match e.args[0]:
        case key if key is You:
          join[val][You] = {Id: False}
        case key if key is val:
          continue

  rm = []

  for val, d in join.items():
    if len(d) < 2:
      rm.append(val)

  for val in rm:
    del join[val]

  return join

while True:
  try:
    t = stdin.readline()
    if len(t) > 0:
      t = tuple(t.strip().split())
    else:
      break

    match t:
      case ('IdMe','StrMe','Int'):
        curr = me
      case ('IdYou','StrYou','Int'):
        curr = you
      case (Id, name, val):
        Id = int(Id)
        val = int(val)
        curr[Id] = val
      case ('IdMe','StrMe','Int','IdYou','StrYou'):
        join = natural_join(me, you)
      case (Me_Id, Me_name, val, You_Id, You_name):
        Me_Id = int(Me_Id)
        val = int(val)
        You_Id = int(You_Id)
        if not me[Me_Id] == you[You_Id] == val:
          print(f'found inconsistency;')
          print(f'value {me[Me_Id]  } of table Me  (id {Me_Id }) was mapped to')
          print(f'value {you[You_Id]} of table You (id {You_Id}) with value {val}')
          exit(1)
        
        join[val][Me][Me_Id]   = True
        join[val][You][You_Id] = True
        

  except EOFError:
    print('Reached eof')
    exit(0)

  except ValueError:
    continue

for val in join:
  for table in join[val]:
    for id in join[val][table]:
      if not join[val][table][id]:
        print(f'relation not satisfied for row {id} in table {table} with value {val}')

print('Looks good to me!')