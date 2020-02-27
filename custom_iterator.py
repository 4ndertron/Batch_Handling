lst = ['string', 'bacon', 2, 45]

iter_lst = iter(lst)

while True:
    try:
        o = next(iter_lst)
        print(o)
    except StopIteration:
        print('loop broken')
        break
