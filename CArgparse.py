import argparse


class CArgparse:

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.addArgument()

    def addArgument(self):
        """
        Определяет параметры командной строки
        :return: object parser
        """
        self.parser.add_argument('-scenario', choices=['cron', 'once'],
                                 default='once', help='cron - запускает скрипт по времени, once - запускает один раз')
        self.parser.add_argument('-url', type=str, help="URL")
        self.parser.add_argument('-deep', type=int, help="Deep of search")
        self.parser.add_argument('-sort', default="date", type=str, help="Sort of search")



    def argsAsDict(self):
        args = self.parser.parse_args()._get_kwargs()
        argsdict = {}
        for key, value in args:
            if value is not None:
                argsdict[key] = value
        return argsdict
