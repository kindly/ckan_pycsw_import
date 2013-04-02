import logging
import sys
import requests
from pycsw import metadata, repository, util
from lxml import etree
import urlparse
import datetime
import io
from pycsw import config

logging.basicConfig(format='%(message)s', level=logging.INFO)

LOGGER = logging.getLogger(__name__)

def setup_db(database, table):
    """Setup database tables and indexes"""
    from sqlalchemy import Column, create_engine, Integer, MetaData, \
        Table, Text

    LOGGER.info('Creating database %s', database)
    dbase = create_engine(database)

    mdata = MetaData(dbase)

    LOGGER.info('Creating table %s', table)
    records = Table(
        table, mdata,
        # core; nothing happens without these
        Column('identifier', Text, primary_key=True),
        Column('typename', Text,
               default='csw:Record', nullable=False, index=True),
        Column('schema', Text,
               default='http://www.opengis.net/cat/csw/2.0.2', nullable=False,
               index=True),
        Column('mdsource', Text, default='local', nullable=False,
               index=True),
        Column('insert_date', Text, nullable=False, index=True),
        Column('xml', Text, nullable=False),
        Column('anytext', Text, nullable=False),
        Column('language', Text, index=True),

        # identification
        Column('type', Text, index=True),
        Column('title', Text, index=True),
        Column('title_alternate', Text, index=True),
        Column('abstract', Text),
        Column('keywords', Text),
        Column('keywordstype', Text, index=True),
        Column('parentidentifier', Text, index=True),
        Column('relation', Text, index=True),
        Column('time_begin', Text, index=True),
        Column('time_end', Text, index=True),
        Column('topicategory', Text, index=True),
        Column('resourcelanguage', Text, index=True),

        # attribution
        Column('creator', Text, index=True),
        Column('publisher', Text, index=True),
        Column('contributor', Text, index=True),
        Column('organization', Text, index=True),

        # security
        Column('securityconstraints', Text),
        Column('accessconstraints', Text),
        Column('otherconstraints', Text),

        # date
        Column('date', Text, index=True),
        Column('date_revision', Text, index=True),
        Column('date_creation', Text, index=True),
        Column('date_publication', Text, index=True),
        Column('date_modified', Text, index=True),

        Column('format', Text, index=True),
        Column('source', Text, index=True),

        # geospatial
        Column('crs', Text, index=True),
        Column('geodescode', Text, index=True),
        Column('denominator', Integer, index=True),
        Column('distancevalue', Integer, index=True),
        Column('distanceuom', Text, index=True),
        Column('wkt_geometry', Text),

        # service
        Column('servicetype', Text, index=True),
        Column('servicetypeversion', Text, index=True),
        Column('operation', Text, index=True),
        Column('couplingtype', Text, index=True),
        Column('operateson', Text, index=True),
        Column('operatesonidentifier', Text, index=True),
        Column('operatesoname', Text, index=True),

        # additional
        Column('degree', Text, index=True),
        Column('classification', Text, index=True),
        Column('conditionapplyingtoaccessanduse', Text, index=True),
        Column('lineage', Text, index=True),
        Column('responsiblepartyrole', Text, index=True),
        Column('specificationtitle', Text, index=True),
        Column('specificationdate', Text, index=True),
        Column('specificationdatetype', Text, index=True),

        # distribution
        # links: format "name,description,protocol,url[^,,,[^,,,]]"
        Column('links', Text, index=True),

        Column('ckan_id', Text, index=True),
        Column('ckan_modified', Text),
    )
    records.create()


def load_ckan(database, ckan_url, table):
    context = config.StaticContext()
    repo = repository.Repository(database, context, table=table)
    ckan_url = ckan_url.lstrip('/') + '/'
    LOGGER.info('gather started')
    LOGGER.info(str(datetime.datetime.now()))

    query = 'api/search/dataset?qjson={"fl":"id,metadata_modified,extras_harvest_object_id,extras_metadata_source", "q":"harvest_object_id:*", "limit":1000, "start":%s}'

    start = 0

    gathered_records = {}

    while True:
        url = ckan_url + query % start

        response = requests.get(url)
        listing = response.json()
        results = listing.get('results')
        if not results:
            break

        for result in results:
            gathered_records[result['id']] = {
                'metadata_modified': result['metadata_modified'],
                'harvest_object_id': result['extras']['harvest_object_id'],
                'source': result['extras'].get('metadata_source')
            }

        start = start + 1000
        LOGGER.info('Gathered %s' % start)

    LOGGER.info('gather finished')
    LOGGER.info(str(datetime.datetime.now()))

    existing_records = {}

    query = repo.session.query(repo.dataset.ckan_id, repo.dataset.ckan_modified)
    for row in query:
        existing_records[row[0]] = row[1]
    repo.session.close()

    new = set(gathered_records) - set(existing_records)
    deleted = set(existing_records) - set(gathered_records)
    changed = set()

    for key in set(gathered_records) & set(existing_records):
        if gathered_records[key]['metadata_modified'] > existing_records[key]:
            changed.add(key)

    for ckan_id in deleted:
        try:
            repo.session.begin()
            rows = repo.session.query(repo.dataset.ckan_id).filter_by(
            ckan_id=ckan_id).delete()
            LOGGER.info('Deleted %s' % ckan_id)
            repo.session.commit()
        except Exception, err:
            repo.session.rollback()
            raise

    for ckan_id in new:
        ckan_info = gathered_records[ckan_id]
        record = get_record(context, repo, ckan_url, ckan_id, ckan_info)
        if not record:
            LOGGER.info('skipped record %s' % ckan_id)
            continue
        try:
            repo.insert(record, 'local', util.get_today_and_now())
            LOGGER.info('Inserted %s' % ckan_id)
        except Exception, err:
            LOGGER.error('ERROR: not inserted %s Error:%s' % (ckan_id, err))

    for ckan_id in changed:
        ckan_info = gathered_records[ckan_id]
        record = get_record(context, repo, ckan_url, ckan_id, ckan_info)
        if not record:
            continue
        update_dict = dict([(getattr(repo.dataset, key),
        getattr(record, key)) \
        for key in record.__dict__.keys() if key != '_sa_instance_state'])
        try:
            repo.session.begin()
            repo.session.query(repo.dataset).filter_by(
            ckan_id=ckan_id).update(update_dict)
            repo.session.commit()
            LOGGER.info('Changed %s' % ckan_id)
        except Exception, err:
            repo.session.rollback()
            raise RuntimeError, 'ERROR: %s' % str(err)


def get_record(context, repo, ckan_url, ckan_id, ckan_info):
    query = ckan_url + 'harvest/object/%s'
    url = query % ckan_info['harvest_object_id']
    response = requests.get(url)

    if ckan_info['source'] == 'arcgis':
        return

    try:
        xml = etree.parse(io.BytesIO(response.content))
    except Exception, err:
        LOGGER.error('Could not pass xml doc from %s, Error: %s' % (ckan_id, err))
        return

    try:
        record = metadata.parse_record(context, xml, repo)[0]
    except Exception, err:
        LOGGER.error('Could not extract metadata from %s, Error: %s' % (ckan_id, err))
        return

    if not record.identifier:
        record.identifier = ckan_id
    record.ckan_id = ckan_id
    record.ckan_modified = ckan_info['metadata_modified']

    return record


if __name__ == '__main__':

    if sys.argv[1] == 'setup':
        setup_db(sys.argv[2], 'records')

    if sys.argv[1] == 'load':
        load_ckan(sys.argv[2], sys.argv[3], 'records')






