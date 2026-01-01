/***** パラメータ *****/
var START_YEAR = 2019;
var END_YEAR_JS = new Date().getFullYear();     // クライアント側の現在年
var END_YEAR = ee.Number(END_YEAR_JS);          // サーバ側でも利用
var SCALE = 5000;   // CHIRPSは~5km格子。重いときは10000に
var TILE  = 4;

/***** 境界（GAUL ADM2：インドネシア、簡略化）*****/
var ADM2 = ee.FeatureCollection('FAO/GAUL/2015/level2')
  .filter(ee.Filter.eq('ADM0_NAME','Indonesia'))
  .map(function(f){ return f.simplify(100); });
var GEOM = ADM2.geometry();

/***** データ：CHIRPS 日次降水（mm/day）*****/
var CHIRPS = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY').select('precipitation');

/***** 年ごとの集計関数 *****/
function statsByYear(y){
  y = ee.Number(y);

  // 1) カレンダー年 合計(mm)
  var yStart = ee.Date.fromYMD(y,1,1);
  var yEnd   = yStart.advance(1,'year');
  var annual = CHIRPS.filterDate(yStart, yEnd).sum().clip(GEOM); // mm

  var annualStats = annual.reduceRegions({
    collection: ADM2,
    reducer: ee.Reducer.mean(),   // 格子平均（mm）
    scale: SCALE,
    tileScale: TILE,
    // 必要ならアンコメント：maxPixelsPerRegion: 1e9
  }).map(function(f){
    return f.set({
      year: y,
      calendar_year_mm: ee.Number(f.get('mean'))
    });
  });

  // 2) 雨季（前年11月〜当年3月）合計(mm)
  var wetStart = ee.Date.fromYMD(y.subtract(1),11,1);
  var wetEnd   = ee.Date.fromYMD(y,4,1); // 3月末の翌日
  var wet = CHIRPS.filterDate(wetStart, wetEnd).sum().clip(GEOM);

  var wetStats = wet.reduceRegions({
    collection: ADM2,
    reducer: ee.Reducer.mean(),
    scale: SCALE,
    tileScale: TILE,
    // 必要ならアンコメント：maxPixelsPerRegion: 1e9
  }).map(function(f){
    return f.set({
      year: y,
      rainy_season_mm: ee.Number(f.get('mean'))
    });
  });

  // annual と wet を ADM2_CODEで結合（nullガード付き）
  var joined = ee.Join.saveFirst('wet').apply(
    annualStats, wetStats,
    ee.Filter.equals({leftField:'ADM2_CODE', rightField:'ADM2_CODE'})
  );

  return ee.FeatureCollection(joined.map(function(f){
    var wetF = ee.Feature(f.get('wet'));
    var rainy = ee.Algorithms.If(wetF, wetF.get('rainy_season_mm'), null);
    return ee.Feature(null, {
      ADM1_NAME: f.get('ADM1_NAME'),
      ADM2_CODE: f.get('ADM2_CODE'),
      ADM2_NAME: f.get('ADM2_NAME'),
      year:      f.get('year'),
      calendar_year_mm: f.get('calendar_year_mm'),
      rainy_season_mm:  rainy
    });
  }));
}

/***** 実行・テーブル作成 *****/
var years = ee.List.sequence(START_YEAR, END_YEAR);
var rainTable = ee.FeatureCollection(years.map(statsByYear))
  .flatten()
  .select(['ADM1_NAME','ADM2_CODE','ADM2_NAME','year','calendar_year_mm','rainy_season_mm']);

/***** エクスポート（getInfoを使わない）*****/
Export.table.toDrive({
  collection: rainTable,
  description: 'IDN_CHIRPS_ADM2_' + START_YEAR + '_' + END_YEAR_JS,
  fileNamePrefix: 'idn_chirps_adm2_' + START_YEAR + '_' + END_YEAR_JS,
  fileFormat: 'CSV'
});

/***** 確認（軽い表示のみ・任意）*****/
Map.centerObject(ADM2, 5);
Map.addLayer(
  CHIRPS.filterDate(ee.Date.fromYMD(2024,1,1), ee.Date.fromYMD(2025,1,1)).sum().clip(GEOM),
  {min:0, max:4000}, 'CHIRPS 2024 total (mm)'
);

