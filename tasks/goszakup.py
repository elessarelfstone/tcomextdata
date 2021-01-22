import attr
import luigi
from datetime import datetime

from luigi.util import requires

from settings import GOSZAKUP_GQL_TOKEN
from tasks.base import LoadingDataIntoCsvFile, Runner, GzipAndUploadToFtp, BigDataToCsv
from tcomextdata.common.dates import DEFAULT_DATE_FORMAT, prevday
from tcomextdata.common.csv import save_csvrows
from tcomextdata.common.utils import append_file
from tcomextdata.goszakup import goszakup_graphql_parsing, goszakup_rest_parsing


@attr.s
class GoszakupUntrustedSupplierRow:
    """ Structure for wrapping records with untrusted suppliers """
    pid = attr.ib(default='')
    supplier_biin = attr.ib(default='')
    supplier_innunp = attr.ib(default='')
    supplier_name_ru = attr.ib(default='')
    supplier_name_kz = attr.ib(default='')
    kato_list = attr.ib(default='')
    index_date = attr.ib(default='')
    system_id = attr.ib(default='')


@attr.s
class GoszakupCompanyRow:
    """ Structure for wrapping records with companies """
    pid = attr.ib(default='')
    bin = attr.ib(default='')
    iin = attr.ib(default='')
    inn = attr.ib(default='')
    unp = attr.ib(default='')
    regdate = attr.ib(default='')
    crdate = attr.ib(default='')
    number_reg = attr.ib(default='')
    series = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    email = attr.ib(default='')
    phone = attr.ib(default='')
    website = attr.ib(default='')
    last_update_date = attr.ib(default='')
    country_code = attr.ib(default='')
    qvazi = attr.ib(default='')
    customer = attr.ib(default='')
    organizer = attr.ib(default='')
    mark_national_company = attr.ib(default='')
    ref_kopf_code = attr.ib(default='')
    mark_assoc_with_disab = attr.ib(default='')
    year = attr.ib(default='')
    mark_resident = attr.ib(default='')
    system_id = attr.ib(default='')
    supplier = attr.ib(default='')
    krp_code = attr.ib(default='')
    oked_list = attr.ib(default='')
    kse_code = attr.ib(default='')
    mark_world_company = attr.ib(default='')
    mark_state_monopoly = attr.ib(default='')
    mark_natural_monopoly = attr.ib(default='')
    mark_patronymic_producer = attr.ib(default='')
    mark_patronymic_supplyer = attr.ib(default='')
    mark_small_employer = attr.ib(default='')
    type_supplier = attr.ib(default='')
    is_single_org = attr.ib(default='')
    index_date = attr.ib(default='')


@attr.s
class GoszakupContractRow:
    """ Structure for wrapping records with contracts """
    id = attr.ib(default='')
    parent_id = attr.ib(default='')
    root_id = attr.ib(default='')
    trd_buy_id = attr.ib(default='')
    trd_buy_number_anno = attr.ib(default='')
    trd_buy_name_ru = attr.ib(default='')
    trd_buy_name_kz = attr.ib(default='')
    ref_contract_status_id = attr.ib(default='')
    deleted = attr.ib(default='')
    crdate = attr.ib(default='')
    last_update_date = attr.ib(default='')
    supplier_id = attr.ib(default='')
    supplier_biin = attr.ib(default='')
    supplier_bik = attr.ib(default='')
    supplier_iik = attr.ib(default='')
    supplier_bank_name_kz = attr.ib(default='')
    supplier_bank_name_ru = attr.ib(default='')
    supplier_legal_address = attr.ib(default='')
    supplier_bill_id = attr.ib(default='')
    contract_number = attr.ib(default='')
    sign_reason_doc_name = attr.ib(default='')
    sign_reason_doc_date = attr.ib(default='')
    trd_buy_itogi_date_public = attr.ib(default='')
    customer_id = attr.ib(default='')
    customer_bin = attr.ib(default='')
    customer_bik = attr.ib(default='')
    customer_iik = attr.ib(default='')
    customer_bill_id = attr.ib(default='')
    customer_bank_name_kz = attr.ib(default='')
    customer_bank_name_ru = attr.ib(default='')
    customer_legal_address = attr.ib(default='')
    contract_number_sys = attr.ib(default='')
    payments_terms_ru = attr.ib(default='')
    payments_terms_kz = attr.ib(default='')
    ref_subject_type_id = attr.ib(default='')
    ref_subject_types_id = attr.ib(default='')
    is_gu = attr.ib(default='') # integer
    fin_year = attr.ib(default='') # integer
    ref_contract_agr_form_id = attr.ib(default='')
    ref_contract_year_type_id = attr.ib(default='')
    ref_finsource_id = attr.ib(default='')
    ref_currency_code = attr.ib(default='')
    exchange_rate = attr.ib(default='')
    contract_sum = attr.ib(default='')
    contract_sum_wnds = attr.ib(default='')
    sign_date = attr.ib(default='')
    ec_end_date = attr.ib(default='')
    plan_exec_date = attr.ib(default='')
    fakt_exec_date = attr.ib(default='')
    fakt_sum = attr.ib(default='')
    fakt_sum_wnds = attr.ib(default='')
    contract_end_date = attr.ib(default='')
    ref_contract_cancel_id = attr.ib(default='')
    ref_contract_type_id = attr.ib(default='')
    description_kz = attr.ib(default='')
    description_ru = attr.ib(default='')
    fakt_trade_methods_id = attr.ib(default='')
    ec_customer_approve = attr.ib(default='')
    ec_supplier_approve = attr.ib(default='')
    contract_ms = attr.ib(default='')
    treasure_req_num = attr.ib(default='')
    treasure_req_date = attr.ib(default='')
    treasure_not_num = attr.ib(default='')
    treasure_not_date = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')


class GoszakupGraphQLParsing(LoadingDataIntoCsvFile):
    """ Base class to parse GraphQl data from goszakup.gov.kz"""

    entity_name = luigi.Parameter()
    # dates range
    start_date = luigi.Parameter()
    end_date = luigi.Parameter()
    # portion for each request
    limit = luigi.IntParameter()
    url = luigi.IntParameter()
    # given during registration
    token = luigi.IntParameter(significant=False, default=GOSZAKUP_GQL_TOKEN)
    timeout = luigi.IntParameter(default=0)
    key_column = luigi.Parameter()
    retries = luigi.IntParameter(default=3)
    # header = luigi.Parameter(default='')


class GoszakupRESTDataParsing(LoadingDataIntoCsvFile):
    """ Base class of tasks parsing REST data from goszakup.gov.kz
        for given entity(url).
    """

    # portion for each request
    limit = luigi.IntParameter()
    url = luigi.IntParameter()
    token = luigi.IntParameter(significant=False, default=GOSZAKUP_GQL_TOKEN)
    timeout = luigi.IntParameter(default=0)


class GoszakupRESTBigDataParsing(GoszakupRESTDataParsing, BigDataToCsv):
    """ Base class of tasks parsing all REST data from goszakup.gov.kz
        for given entity(url). Use for long proccess where it comes with big data.
        Luigi progress bar(gui) could be using to see current status and percentage.
    """
    pass


class GoszakupUntrustedSuppliersParsing(GoszakupRESTDataParsing):

    only_csv = luigi.BoolParameter(default=True)

    def run(self):
        stat = goszakup_rest_parsing(self.output().path, self.url, self.token,
                                     self.timeout, self.limit,
                                     struct=GoszakupUntrustedSupplierRow,
                                     callb_luigi_status=self.set_status)


class GoszakupAllCompaniesParsing(GoszakupRESTBigDataParsing):

    def run(self):
        stat = goszakup_rest_parsing(self.output().path, self.url, self.token,
                                     self.timeout, self.limit,
                                     struct=GoszakupCompanyRow,
                                     prs_fpath=self.parsed_fpath,
                                     callb_luigi_status=self.set_status)
        append_file(self.success_fpath, stat)


@requires(GoszakupAllCompaniesParsing)
class UploadGoszakupAllCompanies(GzipAndUploadToFtp):
    """ Gzip and upload csv with companies to ftp """
    pass


@requires(GoszakupUntrustedSuppliersParsing)
class UploadGoszakupUntrustedSuppliers(GzipAndUploadToFtp):
    """ Gzip and upload csv with companies to ftp """
    pass


class GoszakupCompaniesParsing(GoszakupGraphQLParsing):
    """ Parsing companies to csv"""
    def run(self):

        data = goszakup_graphql_parsing(self.entity_name, self.url, self.token,
                                        self.start_date, self.end_date,
                                        self.limit, self.timeout, self.key_column,
                                        self.retries, struct=GoszakupCompanyRow)

        # for this entity we save header
        # requested by Esmukhanov
        # could be removed in future
        header = [tuple(f.name for f in attr.fields(GoszakupCompanyRow))]
        save_csvrows(self.output().path, header,
                     sep=self.sep)

        save_csvrows(self.output().path, data, sep=self.sep)


class GoszakupContractsParsing(GoszakupGraphQLParsing):
    """ Parsing contracts to csv"""
    def run(self):

        data = goszakup_graphql_parsing(self.entity_name, self.url, self.token,
                                        self.start_date, self.end_date,
                                        self.limit, self.timeout, self.key_column,
                                        self.retries, struct=GoszakupContractRow)

        save_csvrows(self.output().path, data, sep=self.sep)


class GoszakupGraphQlRunner(Runner):

    # by default we parse data only for yesterday
    start_date = luigi.DateParameter(default=prevday(1))
    end_date = luigi.DateParameter(default=prevday(1))

    @property
    def days_params(self):

        return {'start_date': datetime.strftime(self.start_date, DEFAULT_DATE_FORMAT),
                'end_date': datetime.strftime(self.end_date, DEFAULT_DATE_FORMAT)
                }


@requires(GoszakupCompaniesParsing)
class UploadGoszakupCompanies(GzipAndUploadToFtp):
    """ Gzip and upload csv with companies to ftp"""
    pass


@requires(GoszakupContractsParsing)
class UploadGoszakupContracts(GzipAndUploadToFtp):
    """ Gzip and upload csv with contracts to ftp"""
    pass


class GoszakupCompanies(GoszakupGraphQlRunner):
    """ Running task for parsing compnaies """
    name = luigi.Parameter(default='goszakup_companies')

    def requires(self):
        return UploadGoszakupCompanies(**self.days_params, **self.params)


class GoszakupAllCompanies(Runner):
    """ Running task for parsing compnaies """
    name = luigi.Parameter(default='goszakup_all_companies')

    def requires(self):
        return UploadGoszakupAllCompanies(**self.params)


class GoszakupContracts(GoszakupGraphQlRunner):
    """ Running task for parsing contracts """

    name = luigi.Parameter(default='goszakup_contracts')

    def requires(self):
        return UploadGoszakupContracts(**self.days_params, **self.params)


class GoszakupUntrustedSuppliers(Runner):

    name = luigi.Parameter(default='goszakup_untrusted')

    def requires(self):
        return UploadGoszakupUntrustedSuppliers(**self.params)


if __name__ == '__main__':
    luigi.run()



