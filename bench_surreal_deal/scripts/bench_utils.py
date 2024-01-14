from random import Random
from uuid import UUID

def generate_uuid4(amount, seed=42):
    """Yields a generator object of pseudorandom uuids"""
    rnd = Random()
    rnd.seed(seed)
    return (UUID(int=rnd.getrandbits(128), version=4) for _ in range(amount))

def generate_random_number_list(total_num, list_num, seed=42):
    """Returns a list of unique pseudorandomumbers"""
    rnd = Random()
    rnd.seed(seed)
    rnd_num_list = []
    while len(rnd_num_list) <= list_num:
        number = rnd.randint(0, total_num) 
        if number not in rnd_num_list:
            rnd_num_list.append(number)
    sorted_rnd_num_list = sorted(rnd_num_list)
    return sorted_rnd_num_list

def get_gen_uuid4_unique_list(total_num, list_num, seed=42):
    """Returns 10 randomly chosen uuids from a pseudorandom generator"""
    # Generating the uuids
    uuid_gen = generate_uuid4(total_num, seed)

    # Generating a list of unique random numbers
    num_list = generate_random_number_list(total_num, list_num, seed)
    
    # gathering uudids into a list
    num = 0
    uuid_list = []
    for _ in range(total_num):
        if num in num_list:
            uuid_list.append(next(uuid_gen))
            num +=1
        else:
            next(uuid_gen)
            num +=1
    return uuid_list