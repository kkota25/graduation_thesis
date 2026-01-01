/***** 設定 *****/
var SCALE = 2000;   // まだ重ければ 3000–4000 に
var TILE  = 8;      // タイル分割でメモリ節約

/***** 境界（GAUL ADM2：インドネシア）*****/
var ADM2 = ee.FeatureCollection('FAO/GAUL/2015/level2')
  .filter(ee.Filter.eq('ADM0_NAME','Indonesia'))
  .map(function(f){ return f.simplify(500); });   // 形状簡略化

/***** Sentinel-2 Cloud Probability *****/
var S2_CP = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY'); // band: probability(0–100)

/***** 島嶼ブロック（GAUL 2015 の ADM1 名称ベース）*****/
var REGION_DEF = {
  'Sumatra': [
    'Aceh','Sumatera Utara','Sumatera Barat','Riau','Kepulauan Riau','Jambi',
    'Sumatera Selatan','Kepulauan Bangka Belitung','Bengkulu','Lampung'
  ],
  'Java_Bali': [
    'Banten','DKI Jakarta','Jawa Barat','Jawa Tengah','DI Yogyakarta','Jawa Timur','Bali'
  ],
  'Kalimantan': [
    'Kalimantan Barat','Kalimantan Tengah','Kalimantan Selatan','Kalimantan Timur','Kalimantan Utara'
  ],
  'Sulawesi': [
    'Sulawesi Utara','Gorontalo','Sulawesi Tengah','Sulawesi Barat','Sulawesi Selatan','Sulawesi Tenggara'
  ],
  'Maluku_NT': [
    'Maluku','Maluku Utara','Nusa Tenggara Barat','Nusa Tenggara Timur'
  ],
  'Papua': [
    // GAUL2015 では分割前の州名（Papua, Papua Barat）
    'Papua','Papua Barat'
  ]
};

/***** 年リスト（必要な年だけ指定）*****/
var YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025];

/***** 島（region）ごとに、全年分を1つの CSV にまとめてエクスポート *****/
Object.keys(REGION_DEF).forEach(function(region){
  var adm1Names = REGION_DEF[region];
  var adm2_sub  = ADM2.filter(ee.Filter.inList('ADM1_NAME', adm1Names));
  var geom_sub  = adm2_sub.geometry();

  // この region 内の 2019–2025 年の結果をマージしていくコンテナ
  var allStats = ee.FeatureCollection([]);

  YEARS.forEach(function(year){
    var start = ee.Date.fromYMD(year, 1, 1);
    var end   = ee.Date.fromYMD(year + 1, 1, 1);  // 翌年1/1まで（≒その年の12/31まで）

    // その年の cloud probability（0–1）平均
    var cpYear = S2_CP
      .filterDate(start, end)
      .filterBounds(geom_sub)
      .select('probability')
      .mean()
      .divide(100);  // 0–1

    // ADM2 ごとの空間平均
    var statsYear = cpYear.reduceRegions({
      collection: adm2_sub,
      reducer: ee.Reducer.mean(),
      scale: SCALE,
      tileScale: TILE
    })
    .map(function(f){
      var m = ee.Number(f.get('mean'));
      return f.set({
        year: year,
        cloud_share: m,                         // 年平均の雲率（0–1）
        clear_share: ee.Number(1).subtract(m)   // 年平均の晴天率
      })
      .select(['ADM1_NAME','ADM2_CODE','ADM2_NAME','year','cloud_share','clear_share']);
    });

    // この region の FeatureCollection にマージ
    allStats = allStats.merge(statsYear);
  });

  // 島ごと（regionごと）に 2019–2025 の全てを 1 CSV でエクスポート
  var desc = 'IDN_cloud_ADM2_' + region + '_2019_2025';

  Export.table.toDrive({
    collection: allStats,
    description: desc,
    fileNamePrefix: desc,
    fileFormat: 'CSV'
  });
});

/***** ざっくり可視化（任意。重ければコメントアウト）*****/
// 例として 2019 年だけ可視化
var previewYear = 2019;
var previewImg = S2_CP
  .filterDate(ee.Date.fromYMD(previewYear,1,1), ee.Date.fromYMD(previewYear+1,1,1))
  .select('probability')
  .mean()
  .divide(100)
  .clip(ADM2.geometry());

Map.addLayer(previewImg, {min:0, max:1}, 'Cloud prob ' + previewYear);
Map.centerObject(ADM2.first(), 5);

