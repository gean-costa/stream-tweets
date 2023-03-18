from multiprocessing import Pool




import math



def x_vez_2(x):
    for xi in x:
        print(xi*2)

tweets_ids = list(range(230))

def reshaper(lista):
    if len(lista)>100:
        num_blocks = math.ceil(len(lista)/100)
        return [lista[(a-1)*100:a*100] for a in range(1,num_blocks+1)]
    return lista

print(reshaper(tweets_ids))



