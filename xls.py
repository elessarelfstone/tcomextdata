import luigi

from tcomextdata.tasks.companies import Companies
from tcomextdata.tasks.oked import Oked
from tcomextdata.tasks.kato import Kato
from tcomextdata.tasks.kpved import Kpved


class XlsTasksRunner(luigi.WrapperTask):
    def requires(self):
        yield Oked()
        yield Kato()
        yield Companies()
        yield Kpved()


if __name__ == '__main__':
    luigi.run()
