from mysql.connector.errors import InternalError

def decorator_factory(retry, exception_class):
    def decorator(function):
        def wrapper(*args, **kwargs):
            for i in range(retry):
                try:
                    return function(*args, **kwargs)            
                except exception_class as e:
                    if i + 1 == retry:
                        print('done {} tryouts'.format(retry))
                        raise e            
        return wrapper
    return decorator

@decorator_factory(retry=5, exception_class=InternalError)
def another_func(a, b):
    print('another_func')
    raise Exception('test')
    return 124
#    raise InternalError('test error')  
    
    
if __name__ == '__main__':
    import indi.indpool
    
    print(indi.indpool.one_pass(2))
    