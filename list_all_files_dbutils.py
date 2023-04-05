def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)
    
my_list =list(get_dir_content('/mnt/produced/'))
#matchers = ['.csv']
#matching = [s for s in my_list if any(xs in s for xs in matchers)]
#print(matching)
