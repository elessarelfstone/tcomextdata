import luigi

from tcomextdata.tasks.oked import Oked


class OkedRunner(luigi.WrapperTask):
    def requires(self):
        return Oked()


if __name__ == '__main__':
    luigi.run()
