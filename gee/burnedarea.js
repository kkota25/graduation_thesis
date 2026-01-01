/***** パラメータ *****/
var START_YEAR = 2019;
var END_YEAR_JS = new Date().getFullYear();      // 例: 2025
var END_YEAR = ee.Number(END_YEAR_JS);

var SCALE = 500;   // MCD64A1は500m。重ければ 1000 へ
var TILE  = 4;     // 重ければ 8 へ

/***** 境界（GAUL ADM2：インドネシア、簡略化）*****/
var ADM2 = ee.FeatureCollection('FAO/GAUL/2015/level2')
  .filter(ee.Filter.eq('ADM0_NAME', 'Indonesia'))
  .map(function(f){ return f.simplify(100); });   // 100m簡略化で軽量化
var GEOM = ADM2.geometry();

/***** 島グループ *****/
var GROUPS = {
  'Sumatra': ['Aceh','Sumatera Utara','Sumatera Barat','Riau','Jambi',
              'Sumatera Selatan','Bengkulu','Lampung','Kepulauan Riau','Kepulauan Bangka Belitung'],
  'Java_Bali': ['Daerah Khusus Ibukota Jakarta Raya','Banten','Jawa Barat','Jawa Tengah',
                'Jawa Timur','Daerah Istimewa Yogyakarta','Bali'],
  'Kalimantan': ['Kalimantan Barat','Kalimantan Tengah','Kalimantan Selatan','Kalimantan Timur','Kalimantan Utara'],
  'Sulawesi': ['Sulawesi Utara','Gorontalo','Sulawesi Tengah','Sulawesi Barat','Sulawesi Selatan','Sulawesi Tenggara'],
  'Maluku_NT': ['Maluku','Maluku Utara','Nusa Tenggara Barat','Nusa Tenggara Timur'],
  'Papua': ['Papua','Papua Barat','Papua Barat Daya','Papua Pegunungan','Papua Tengah','Papua Selatan']
};

/***** データ：MODIS 焼失面積 *****/
var MCD = ee.ImageCollection('MODIS/006/MCD64A1'); // バンド: 'BurnDate'

/***** 年ごとのADM2×焼失面積(ha)（size()ガード付き）*****/
function burnedYearForSubset(y, subADM2, subGeom){
  y = ee.Number(y);

  var yearCol = MCD
    .filterDate(ee.Date.fromYMD(y,1,1), ee.Date.fromYMD(y.add(1),1,1))
    .select('BurnDate');

  // その年の画像が無ければゼロ画像、あれば BurnDate>0
  var burnedMask = ee.Image(ee.Algorithms.If(
    yearCol.size().gt(0),
    yearCol.max().gt(0),
    ee.Image.constant(0).rename('mask')
  )).clip(subGeom);

  var ha = ee.Image.pixelArea().divide(10000).updateMask(burnedMask);

  var stats = ha.reduceRegions({
    collection: subADM2,
    reducer: ee.Reducer.sum(),
    scale: SCALE,
    tileScale: TILE
    // 必要なら: maxPixelsPerRegion: 1e9
  }).map(function(f){
    var val = ee.Number(ee.Algorithms.If(f.get('sum'), f.get('sum'), 0));
    return ee.Feature(null, {
      ADM1_NAME: f.get('ADM1_NAME'),
      ADM2_CODE: f.get('ADM2_CODE'),
      ADM2_NAME: f.get('ADM2_NAME'),
      year: y,
      burned_ha: val
    });
  });

  return stats;
}

/***** シャード（島×年範囲）ごとにエクスポート *****/
function exportShard(tag, provinces, yStart, yEnd){
  var subADM2 = ADM2.filter(ee.Filter.inList('ADM1_NAME', ee.List(provinces)));
  var subGeom = subADM2.geometry();
  var years = ee.List.sequence(yStart, yEnd);

  var fc = ee.FeatureCollection(
    years.map(function(yy){ return burnedYearForSubset(yy, subADM2, subGeom); })
  ).flatten();

  Export.table.toDrive({
    collection: fc,
    description: 'IDN_burned_ha_ADM2_'+tag+'_'+yStart+'_'+yEnd,
    fileNamePrefix: 'idn_burned_ha_adm2_'+tag+'_'+yStart+'_'+yEnd,
    fileFormat: 'CSV'
  });
}

/***** 実行：2019–2021 / 2022–END を島ごとに分割出力 *****/
var Y1s = 2019, Y1e = 2021;
var Y2s = 2022, Y2e = END_YEAR_JS;

Object.keys(GROUPS).forEach(function(k){
  exportShard(k+'_2019_2021', GROUPS[k], Y1s, Y1e);
  exportShard(k+'_2022_'+END_YEAR_JS, GROUPS[k], Y2s, Y2e);
});

/***** 画面確認（size()ガード付き）*****/
Map.centerObject(ADM2, 5);
var lastYear = END_YEAR;
var lastCol = MCD
  .filterDate(ee.Date.fromYMD(lastYear,1,1), ee.Date.fromYMD(lastYear.add(1),1,1))
  .select('BurnDate');

var burnedMaskLast = ee.Image(ee.Algorithms.If(
  lastCol.size().gt(0),
  lastCol.max().gt(0).clip(GEOM),
  ee.Image.constant(0).rename('mask').clip(GEOM)
));
Map.addLayer(burnedMaskLast, {}, 'Burned mask (last year)');
