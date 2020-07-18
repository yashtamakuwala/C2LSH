def c(data_hash, query_hashes, offset, length, alpha_m):
    count = 0
    k, v = data_hash

    for i in range(length):
        if abs(v[i] - query_hashes[i]) <= offset:
            count += 1

    return count >= alpha_m


########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):

    offset = 0
    length = len(query_hashes)
    filtered_data = None

    while True:
        filtered_data = data_hashes.filter(lambda x: c(x, query_hashes, offset, length, alpha_m))
        count = filtered_data.count()

        if count >= beta_n :
            break
        if offset == 0:
            offset += 1
        else:
            offset *= 2

    low, high = offset//2, offset

    while low <= high:
        mid = (low + high) // 2

        filtered_data = data_hashes.filter(lambda x: c(x, query_hashes, mid, length, alpha_m))
        count = filtered_data.count()

        if count == beta_n:
            break

        elif count < beta_n:
            low = mid + 1

        else:  # count > beta_n
            high = mid - 1

    if count >= beta_n:
        return filtered_data.map(lambda x: x[0])
    else:
        return data_hashes.filter(lambda x: c(x, query_hashes, low, length, alpha_m)).map(lambda x: x[0])
