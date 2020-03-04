import luigi

from tcomextdata.tasks.companies import Companies
from tcomextdata.tasks.oked import Oked
from tcomextdata.tasks.kato import Kato
from tcomextdata.tasks.kpved import Kpved
from tcomextdata.tasks.mkeis import Mkeis
from tcomextdata.tasks.kurk import Kurk
from tcomextdata.tasks.jwaddress import Jwaddress
from tcomextdata.tasks.taxviolators import TaxViolators
from tcomextdata.tasks.debtors150 import Debtors150
from tcomextdata.tasks.refinance import Refinance
from tcomextdata.tasks.pseudocompany import Pseudocompany
from tcomextdata.tasks.mzp import Mzp
from tcomextdata.tasks.mrp import Mrp
from tcomextdata.tasks.invregistration import Invregistration
from tcomextdata.tasks.inactive import Inactive
from tcomextdata.tasks.bankrupt import Bankrupt


class XlsTasksRunner(luigi.WrapperTask):
    def requires(self):
        # yield Kurk()
        # yield Oked()
        # yield Kato()
        # yield Companies()
        # yield Kpved()
        # yield Mkeis()
        # yield Jwaddress()
        # yield TaxViolators()
        # yield Debtors150()
        # yield Refinance()
        # yield Pseudocompany()
        # yield Mzp()
        # yield Mrp()
        # yield Invregistration()
        # yield Inactive()
        yield Bankrupt()


if __name__ == '__main__':
    luigi.run()
