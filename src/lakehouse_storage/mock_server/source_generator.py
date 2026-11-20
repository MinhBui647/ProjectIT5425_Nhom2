"""Bounded raw fixtures for upstream handoffs, not upstream protocol emulators."""
import random

FAO_INDICES = ('Food Price Index', 'Meat Price Index', 'Dairy Price Index',
               'Cereals Price Index', 'Oils Price Index', 'Sugar Price Index')
FARMS = ('MOC_CHAU', 'BA_VI', 'NGHE_AN', 'LAM_DONG', 'CU_CHI')


def external_batches(request, run_id):
    rng = random.Random(request.seed)
    instant = request.start_time.isoformat()
    date = request.start_time.date().isoformat()
    created = {'_created_at': instant, '_base_api': 'https://mock.invalid/raw-handoff'}
    if 'market_prices' in request.sources:
        yield 'market_prices', [dict(EventNumber='400', EventDate=instant,
                                     ProductGroupCode=product, AveragePublishedPrice=str(3000 + rng.randint(1, 500)),
                                     **created) for product in ('WMP', 'SMP', 'AMF')]
    if 'market_indices' in request.sources:
        month = request.start_time.date().replace(day=1).isoformat()
        if request.fao_variant in {'long', 'both'}:
            yield 'market_indices', [dict(source='FAO', index_name='Dairy Price Index',
                                          period_start=month, index_value=130.0 + rng.random(),
                                          base_period='2014-2016', ingested_at=instant, **created)]
        if request.fao_variant in {'wide', 'both'}:
            yield 'market_indices', [dict(Month=month, **{name: str(120 + rng.random()) for name in FAO_INDICES}, **created)]
    if 'weather' in request.sources:
        yield 'weather', [dict(farm_id=farm, observed_at=instant, temperature_celsius=25.0 + rng.random(),
                              relative_humidity_pct=70, precipitation_mm=0.0, **created) for farm in FARMS]
    if 'food_recalls' in request.sources:
        yield 'food_recalls', [dict(recall_number=f'MOCK-{run_id}', report_date=date.replace('-', ''),
                                   classification='Class II', product_description='UHT milk',
                                   reason_for_recall='Undeclared allergen', status='Ongoing', **created)]
    if 'market_reports' in request.sources:
        for section, extra in [('prices', {'price_range': '100-120'}), ('commentary', {'text': 'Mock dairy market'})]:
            yield 'market_reports', [dict(report_id='MOCK-USDA', section=section, published_date=date, **extra, **created)]
