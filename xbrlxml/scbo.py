"""
Statement of Changes in Beneficial Ownership
impolement parser for forms 3, 4, 5 of SEC
"""
import json
import datetime as dt
import lxml.etree  # type: ignore
from typing import (List, Dict, Optional, Callable,
                    Tuple, Iterable, Any, cast, Union, IO)
from collections import namedtuple


class UnitJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dt.date):
            return (str(obj.year).zfill(4) + '-' +
                    str(obj.month).zfill(2) + '-' +
                    str(obj.day).zfill(2))
        if isinstance(obj, Unit):
            return obj.__dict__
        else:
            return json.JSONEncoder.default(super(), obj)


class Unit():
    def __str__(self) -> str:
        return json.dumps(self.__dict__, cls=UnitJsonEncoder, indent=2)

    def __repr__(self) -> str:
        return self.__str__()


class ReportingOwnerId(Unit):
    def __init__(self):
        self.rptOwnerCik = 0
        self.rptOwnerCcc: Optional[str] = None
        self.rptOwnerName: Optional[str] = None


ReportingOwnerAddress = str


class ReportingOwnerRelationship(Unit):
    def __init__(self):
        self.isDirector: Optional[bool] = None
        self.isOfficer: Optional[bool] = None
        self.isTenPercentOwner: Optional[bool] = None
        self.isOther: Optional[bool] = None
        self.officerTitle: Optional[str] = None
        self.otherText: Optional[str] = None


class Issuer(Unit):
    def __init__(self):
        self.issuerCik = 0
        self.issuerTradingSymbol = ""
        self.issuerName: Optional[str] = None


class ReportingOwner(Unit):
    def __init__(self):
        self.reportingOwnerId = ReportingOwnerId()
        self.reportingOwnerAddress: Optional[ReportingOwnerAddress] = None
        self.reportingOwnerRelationship = ReportingOwnerRelationship()


class TransactionAmounts(Unit):
    def __init__(self):
        self.transactionShares: float = 0.0
        self.transactionPricePerShare: Optional[float] = None
        self.transactionAcquiredDisposedCode = 'A'  # [A|D]
        self.footnotes: List[str] = []


class PostTransactionAmounts(Unit):
    def __init__(self):
        self.sharesOwnedFollowingTransaction: Optional[float] = 0.0
        self.valueOwnedFollowingTransaction: Optional[float] = 0.0
        self.footnotes: List[str] = []


class TransactionCoding(Unit):
    def __init__(self):
        self.transactionFormType = ''
        self.transactionCode = ''
        self.equitySwapInvolved: bool = True


TransactionTimeliness = str
OwnershipNature = str


class NonDerivativeTransaction(Unit):
    def __init__(self):
        self.securityTitle: str = ''
        self.transactionDate: dt.date = dt.date.today()
        self.transactionAmounts = TransactionAmounts()
        self.postTransactionAmounts = PostTransactionAmounts()
        self.ownershipNature = OwnershipNature()

        self.deemedExecutionDate: Optional[dt.date] = None
        self.transactionCoding: Optional[TransactionCoding] = None
        self.transactionTimeliness: Optional[TransactionTimeliness] = None


class NonDerivativeHolding(Unit):
    def __init__(self):
        self.securityTitle = ''
        self.postTransactionAmounts = PostTransactionAmounts()
        self.ownershipNature = OwnershipNature()


class DerivativeHolding(Unit):
    def __init__(self):
        self.securityTitle = ''
        self.conversionOrExercisePrice: Optional[float] = None
        self.exerciseDate: Optional[dt.date] = None
        self.expirationDate: Optional[dt.date] = None
        self.underlyingSecurity = UnderlyingSecurity()
        self.postTransactionAmounts = PostTransactionAmounts()
        self.ownershipNature = OwnershipNature()


class UnderlyingSecurity(Unit):
    def __init__(self):
        self.securityTitle = ''
        self.underlyingSecurityShares: Optional[float] = None
        self.underlyingSecurityValue: Optional[float] = None


class DerivativeTransaction(Unit):
    def __init__(self):
        self.securityTitle = ''
        self.conversionOrExercisePrice: Optional[float] = None
        self.transactionDate: dt.date = dt.date.today()
        self.deemedExecutionDate: Optional[dt.date] = None
        self.transactionCoding: Optional[TransactionCoding] = None
        self.transactionTimeliness: Optional[TransactionTimeliness] = None
        self.transactionAmounts = TransactionAmounts()
        self.exerciseDate: Optional[dt.date] = None
        self.expirationDate: Optional[dt.date] = None
        self.underlyingSecurity = UnderlyingSecurity()
        self.postTransactionAmounts = PostTransactionAmounts()
        self.ownershipNature = OwnershipNature()


class Document(Unit):
    def __init__(self):
        self.documentType = ""
        self.periodOfReport = dt.date.today()
        self.issuer = Issuer()
        self.reportingOwner: List[ReportingOwner] = []
        self.nonDerivativeTable = []
        self.nonDerivativeHolding = []
        self.derivativeTable = []
        self.derivativeHolding = []
        self.footnotes = []
        self.remarks = []


class Parser(object):
    def __init__(self):
        pass

    def find(elem: lxml.etree._Element,
             tag_seq: Iterable[str]) -> lxml.etree._Element:

        for tag in tag_seq:
            elem = elem.find(tag)
            if elem is None:
                raise KeyError(f'{tag} not found')

        return elem

    def parse_str_strict(elem: lxml.etree._Element, tag: str) -> str:
        s = Parser.parse_str(elem, tag)
        if s is None:
            raise KeyError(f'{tag} not found')

        return s

    def parse_str(elem: lxml.etree._Element, tag: str) -> Optional[str]:
        try:
            e = elem.find(tag).text.strip()
            if e == '':
                try:
                    e = elem.find(tag).find('value').text.strip()
                except Exception:
                    pass
            return cast(str, e)
        except Exception:
            return None

    def parse_int_strict(elem: lxml.etree._Element, tag: str) -> int:
        v = Parser.parse_int(elem, tag)
        if v is None:
            raise KeyError(f'{tag} not found')
        return v

    def parse_int(elem: lxml.etree._Element, tag: str) -> Optional[int]:
        try:
            v = Parser.parse_str(elem, tag)
            if v is not None:
                return int(v)
            else:
                return None
        except Exception:
            return None

    def parse_bool(elem: lxml.etree._Element, tag: str) -> Optional[bool]:
        e = Parser.parse_str(elem, tag)
        if e is None:
            return None

        e = e.lower()
        if e == 'true':
            return True
        if e == 'false':
            return False
        if e == '1':
            return True
        if e == '0':
            return False

        return None

    def parse_bool_strict(elem: lxml.etree._Element, tag: str) -> bool:
        try:
            v = Parser.parse_bool(elem, tag)
            if v is not None:
                return v
        except Exception:
            pass

        raise KeyError(f'{tag} not found')

    def parse_float_strict(elem: lxml.etree._Element, tag: str) -> float:
        v = Parser.parse_float(elem, tag)
        if v is None:
            raise KeyError(f'{tag} not found')
        return v

    def parse_float(elem: lxml.etree._Element, tag: str) -> Optional[float]:
        try:
            v = Parser.parse_str(elem, tag)
            if v is not None:
                return float(v)
            else:
                return None
        except Exception:
            return None

    def parse_date_strict(elem: lxml.etree._Element, tag: str) -> dt.date:
        v = Parser.parse_date(elem, tag)
        if v is None:
            raise KeyError(f'{tag} not found')
        else:
            return v

    def parse_date(elem: lxml.etree._Element, tag: str) -> Optional[dt.date]:
        try:
            s = Parser.parse_str(elem, tag)
            if s is not None:
                e = s.split('-')
                return dt.date(int(e[0]), int(e[1]), int(e[2]))
            else:
                return None
        except Exception:
            return None

    def parse_issuer(issuer: lxml.etree._Element) -> Issuer:
        iss = Issuer()

        iss.issuerCik = Parser.parse_int_strict(issuer, 'issuerCik')
        iss.issuerTradingSymbol = Parser.parse_str_strict(
            issuer, 'issuerTradingSymbol')
        iss.issuerName = Parser.parse_str(issuer, 'issuerName')

        return iss

    def parse_owner(owner: lxml.etree._Element) -> ReportingOwner:
        o = ReportingOwner()
        o.reportingOwnerId = Parser.parse_owner_id(
            Parser.find(owner, ['reportingOwnerId'])
        )
        o.reportingOwnerRelationship = Parser.parse_owner_relationship(
            Parser.find(owner, ['reportingOwnerRelationship'])
        )

        address = owner.find('reportingOwnerAddress')
        if address is not None:
            o.reportingOwnerAddress = Parser.parse_address(address)

        return o

    def parse_owner_id(owner_id: lxml.etree._Element) -> ReportingOwnerId:
        r = ReportingOwnerId()
        r.rptOwnerCik = Parser.parse_int_strict(owner_id, 'rptOwnerCik')
        r.rptOwnerCcc = Parser.parse_str(owner_id, 'rptOwnerCcc')
        r.rptOwnerName = Parser.parse_str(owner_id, 'rptOwnerName')

        return r

    def parse_owner_relationship(
            rel: lxml.etree._Element) -> ReportingOwnerRelationship:
        r = ReportingOwnerRelationship()

        values = [
            'isDirector',
            'isOfficer',
            'isTenPercentOwner',
            'isOther',
            'officerTitle',
            'otherText']
        for v in values:
            if v.startswith('is'):
                r.__dict__[v] = Parser.parse_bool(rel, v)
            else:
                r.__dict__[v] = Parser.parse_str(rel, v)

        return r

    def parse_address(address: lxml.etree._Element) -> str:
        return "unimplemented"

    def parse_document(root: lxml.etree._Element) -> Document:
        d = Document()

        d.documentType = Parser.parse_str_strict(root, 'documentType')
        d.periodOfReport = Parser.parse_date_strict(root, 'periodOfReport')
        d.issuer = Parser.parse_issuer(
            Parser.find(root, ['issuer'])
        )

        owners = root.findall('reportingOwner')
        if not owners:
            raise KeyError('reportingOwner not found')

        for owner in owners:
            d.reportingOwner.append(Parser.parse_owner(owner))

        table = root.find('nonDerivativeTable')
        if table is not None:
            trans = table.findall('nonDerivativeTransaction')
            for t in trans:
                d.nonDerivativeTable.append(
                    Parser.parse_nonderivative_transaction(t))

            holds = table.findall('nonDerivativeHolding')
            for h in holds:
                d.nonDerivativeHolding.append(
                    Parser.parse_nonderivative_holding(h))

        table = root.find('derivativeTable')
        if table is not None:
            trans = table.findall('derivativeTransaction')
            for t in trans:
                d.derivativeTable.append(
                    Parser.parse_derivative_transaction(t))

            holds = table.findall('derivativeHolding')
            for h in holds:
                d.derivativeHolding.append(
                    Parser.parse_derivative_holding(h))

        return d

    def parse_transaction_coding(trans: lxml.etree._Element) \
            -> Optional[TransactionCoding]:
        elem = trans.find('transactionCoding')
        if elem is None:
            return None

        tc = TransactionCoding()
        tc.transactionFormType = Parser.parse_str_strict(
            elem, 'transactionFormType')
        tc.transactionCode = Parser.parse_str_strict(elem, 'transactionCode')
        tc.equitySwapInvolved = Parser.parse_bool_strict(
            elem, 'equitySwapInvolved')

        return tc

    def parse_ownership_nature(trans: lxml.etree._Element) -> OwnershipNature:
        elem = Parser.find(trans, ['ownershipNature'])
        return Parser.parse_str_strict(elem, 'directOrIndirectOwnership')

    def parse_tarnsaction_timeliness(trans: lxml.etree._Element) \
            -> Optional[TransactionTimeliness]:
        return Parser.parse_str(trans, 'transactionTimeliness')

    def parse_transaction_amounts(
            trans: lxml.etree._Element) -> TransactionAmounts:
        elem = trans.find('transactionAmounts')
        if elem is None:
            raise KeyError(f'transactionAmounts not found')

        a = TransactionAmounts()
        a.transactionShares = Parser.parse_float_strict(
            elem, 'transactionShares')
        a.transactionPricePerShare = Parser.parse_float(
            elem, 'transactionPricePerShare')
        a.transactionAcquiredDisposedCode = Parser.parse_str_strict(
            elem, 'transactionAcquiredDisposedCode')

        return a

    def parse_post_transaction_amounts(
            trans: lxml.etree._Element) -> PostTransactionAmounts:
        elem = trans.find('postTransactionAmounts')
        if elem is None:
            raise KeyError('postTransactionAmounts not found')

        a = PostTransactionAmounts()
        a.sharesOwnedFollowingTransaction = Parser.parse_float(
            elem, 'sharesOwnedFollowingTransaction')
        a.valueOwnedFollowingTransaction = Parser.parse_float(
            elem, 'valueOwnedFollowingTransaction')

        return a

    def parse_nonderivative_transaction(
            trans: lxml.etree._Element) -> NonDerivativeTransaction:
        t = NonDerivativeTransaction()

        t.securityTitle = Parser.parse_str_strict(trans, 'securityTitle')
        t.transactionDate = Parser.parse_date_strict(trans, 'transactionDate')
        t.deemedExecutionDate = Parser.parse_date(
            trans, 'deemedExecutionDate')
        t.transactionCoding = Parser.parse_transaction_coding(trans)
        t.transactionTimeliness = Parser.parse_tarnsaction_timeliness(trans)
        t.transactionAmounts = Parser.parse_transaction_amounts(trans)
        t.postTransactionAmounts = Parser.parse_post_transaction_amounts(trans)
        t.ownershipNature = Parser.parse_ownership_nature(trans)

        return t

    def parse_underlying_security(
            trans: lxml.etree._Element) -> UnderlyingSecurity:
        elem = Parser.find(trans, ['underlyingSecurity'])
        us = UnderlyingSecurity()
        us.securityTitle = Parser.parse_str_strict(
            elem, 'underlyingSecurityTitle')
        us.underlyingSecurityShares = Parser.parse_float(
            elem, 'underlyingSecurityShares')
        us.underlyingSecurityValue = Parser.parse_float(
            elem, 'underlyingSecurityValue')
        return us

    def parse_derivative_transaction(
            trans: lxml.etree._Element) -> DerivativeTransaction:
        t = DerivativeTransaction()

        t.securityTitle = Parser.parse_str_strict(trans, 'securityTitle')
        t.conversionOrExercisePrice = Parser.parse_float(
            trans, 'conversionOrExercisePrice')
        t.transactionDate = Parser.parse_date_strict(trans, 'transactionDate')
        t.deemedExecutionDate = Parser.parse_date(
            trans, 'deemedExecutionDate')
        t.transactionCoding = Parser.parse_transaction_coding(trans)
        t.transactionTimeliness = Parser.parse_tarnsaction_timeliness(trans)
        t.transactionAmounts = Parser.parse_transaction_amounts(trans)
        t.exerciseDate = Parser.parse_date(trans, 'exerciseDate')
        t.expirationDate = Parser.parse_date(trans, 'expirationDate')
        t.underlyingSecurity = Parser.parse_underlying_security(trans)
        t.postTransactionAmounts = Parser.parse_post_transaction_amounts(trans)
        t.ownershipNature = Parser.parse_ownership_nature(trans)

        return t

    def parse_nonderivative_holding(hold: lxml.etree._Element) \
            -> NonDerivativeHolding:
        h = NonDerivativeHolding()

        h.securityTitle = Parser.parse_str_strict(hold, 'securityTitle')
        h.postTransactionAmounts = Parser.parse_post_transaction_amounts(hold)
        h.ownershipNature = Parser.parse_ownership_nature(hold)

        return h

    def parse_derivative_holding(hold: lxml.etree._Element) \
            -> DerivativeHolding:
        h = DerivativeHolding()

        h.securityTitle = Parser.parse_str_strict(hold, 'securityTitle')
        h.postTransactionAmounts = Parser.parse_post_transaction_amounts(hold)
        h.ownershipNature = Parser.parse_ownership_nature(hold)
        h.underlyingSecurity = Parser.parse_underlying_security(hold)
        h.exerciseDate = Parser.parse_date(hold, 'exerciseDate')
        h.expirationDate = Parser.parse_date(hold, 'expirationDate')
        h.conversionOrExercisePrice = Parser.parse_float(
            hold, 'conversionOrExercisePrice')

        return h


def open_document(filename_or_fileobject: Union[str, IO]) -> Document:
    root = lxml.etree.parse(filename_or_fileobject).getroot()
    return Parser.parse_document(root)


if __name__ == '__main__':
    pass
