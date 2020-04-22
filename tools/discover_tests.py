import mysqlio.firmsio as fio
import unittest


def discover():
    loader = unittest.TestLoader()
    suites = loader.discover(
        "./xbrlxml",
        pattern="test_*.py",
        top_level_dir=".")
    print("start")  # Don't remove this line
    for suite in suites._tests:
        for cls_ in suite._tests:
            try:
                for m in cls_._tests:
                    pass
                    # print(m.id())
            except BaseException:
                print(cls_, 'failed')
                pass


def test():
    df = fio.get_new_companies()


if __name__ == '__main__':
    test()
