/************************************************************
 * インドネシア ADM2 × 2021年 の integrated deforestation alerts (ha)
 *  - Reiche et al. (2024) alert_integration モジュールを利用
 *  - Ruleset-1 (GFW), バッファ処理 OFF（buffer_size=0, temporal_buffer=0）
 *  - reduceRegions の scale は 1000m
 ************************************************************/

// 0. alert_integration モジュール
var alertFunctions = require('users/nrtwur/alert_integration:alert_integration');

// ----------------------------------------------------------
// 1. インドネシア ADM2 境界（GAUL 2015, simplified 500m）
// ----------------------------------------------------------
var adm2 = ee.FeatureCollection('FAO/GAUL_SIMPLIFIED_500m/2015/level2')
  .filter(ee.Filter.eq('ADM0_NAME', 'Indonesia'))
  .select(
    ['ADM1_NAME', 'ADM2_CODE', 'ADM2_NAME'],
    ['adm1_name', 'adm2_code', 'adm2_name']
  );

Map.addLayer(adm2, {}, 'IDN ADM2');
Map.centerObject(adm2, 5);

var idnGeom = adm2.geometry();

// ----------------------------------------------------------
// 2. 元アラートデータの読み込み（読み込み直後に clip）
// ----------------------------------------------------------

// RADD (Sentinel-1) アラート
var RADD_Original = ee.ImageCollection('projects/radar-wur/raddalert/v1')
  .filterMetadata('layer', 'contains', 'alert')
  .mosaic()
  .clip(idnGeom);

// GLAD-Landsat アラート（conf24 が無いので conf25 のみ）
var GLAD_L_Original =
  ee.ImageCollection('projects/glad/alert/2021final')
    .select(['conf21', 'alertDate21']).mosaic()
    .addBands(
      ee.ImageCollection('projects/glad/alert/2022final')
        .select(['conf22', 'alertDate22']).mosaic()
    )
    .addBands(
      ee.ImageCollection('projects/glad/alert/2023final')
        .select(['conf23', 'alertDate23']).mosaic()
    )
    .addBands(
      ee.ImageCollection('projects/glad/alert/UpdResult')
        .select(['conf25', 'alertDate25']).mosaic()
    )
    .clip(idnGeom);

// GLAD-Sentinel-2 アラート
var GLAD_S2_Original =
  ee.Image('projects/glad/S2alert/alert').rename('Alert')
    .addBands(
      ee.Image('projects/glad/S2alert/alertDate').rename('Date')
    )
    .clip(idnGeom);

// ----------------------------------------------------------
// 3. 各プロダクトを共通フォーマット（Alert, Date）に変換
//    Date は「2019-01-01 からの通し日数」
// ----------------------------------------------------------
var RADD    = alertFunctions.convertRADD(RADD_Original);
var GLAD_L  = alertFunctions.convertGLAD_L([GLAD_L_Original]);
var GLAD_S2 = alertFunctions.convertGLAD_S2(GLAD_S2_Original);

// ----------------------------------------------------------
// 4. 3 つのアラートを統合（バッファ処理 OFF）
// ----------------------------------------------------------
var integrated = alertFunctions.integrateAlerts({
  alerts: [RADD, GLAD_L, GLAD_S2],
  ruleset: 'r1',            // Ruleset1 (GFW)
  buffer_size: 0,           // ★ 空間バッファなし
  temporal_buffer: 0,       // ★ 時間バッファなし
  confidence_levels: [2, 3, 4]  // LOW=2, HIGH=3, HIGHEST=4
}).clip(idnGeom);

// 確認用レイヤ（任意）
Map.addLayer(
  integrated.select('Alert'),
  {min: 2, max: 4, palette: ['4BA5FF', 'FF0000', '600000']},
  'Integrated Alert (ruleset1, no buffer)',
  false
);

// ----------------------------------------------------------
// 5. Date バンドを用いて 2021年のアラート面積 (ha) 画像を作成
//    Date: 2019-01-01 を 0 日目とした通し日数
//    2021年 → 730〜1094 日（start=730, end=1095）
// ----------------------------------------------------------
var dateBand = integrated.select('Date');

function makeAreaImageForYear(year) {
  year = ee.Number(year);
  var start = year.subtract(2019).multiply(365); // 2019→0, 2020→365, 2021→730
  var end   = start.add(365);                   // 各年 365 日分

  var maskYear = dateBand.gte(start).and(dateBand.lt(end));

  var areaHa = ee.Image.pixelArea()
    .divide(10000)          // m^2 → ha
    .updateMask(maskYear);

  return areaHa;
}

// 2021年について ADM2 ごとに ha を合計
function aggregateAlertsByAdm2(year) {
  year = ee.Number(year);   // ここでは常に 2021

  var areaHa = makeAreaImageForYear(year);

  var reduced = areaHa.reduceRegions({
    collection: adm2,
    reducer: ee.Reducer.sum().setOutputs(['ha_alerts']),
    scale: 1000,     // ★ 1000m 解像度
    tileScale: 8     // タイルを細かくしてメモリ負荷を軽減
  });

  return reduced.map(function (f) {
    return f
      .set('year', year)
      .set('adm1_name', f.get('adm1_name'))
      .set('adm2_code', f.get('adm2_code'))
      .set('adm2_name', f.get('adm2_name'));
  });
}

// ----------------------------------------------------------
// 6. 2021年のみ ADM2×年（=ADM2×2021）を集計
// ----------------------------------------------------------
var years = ee.List([2021]);
var perYear = years.map(aggregateAlertsByAdm2);
var allYears = ee.FeatureCollection(perYear).flatten();

// デバッグ用 print はコメントアウト
// print('ADM2 × 2021 alerts sample:', allYears.limit(10));

// ----------------------------------------------------------
// 7. Google Drive に CSV としてエクスポート（2021年のみ）
// ----------------------------------------------------------
Export.table.toDrive({
  collection: allYears,
  description: 'idn_integrated_alerts_adm2_yearly_2021',
  fileFormat: 'CSV'
});
