def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    lst_splited = [lst[x:x+n] for x in range(0, len(lst), n)]
    return lst_splited
