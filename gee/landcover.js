/************************************************************
 * インドネシア ADM2 × 年 (2019–2024)
 * MODIS MCD12Q1 土地被覆 IGBP クラス別 ha / share 集計
 *
 *  - データ: MODIS/061/MCD12Q1 (LC_Type1 = IGBP)
 *  - 解像度: 500m
 *  - 出力: ADM2 ごとに
 *      total_ha
 *      ha_water,             share_water
 *      ha_evergreen_needle,  share_evergreen_needle
 *      ...
 *    などクラス別面積とシェアを年ごとに計算
 ************************************************************/

// -----------------------------
// 0. パラメータ
// -----------------------------
var START_YEAR = 2019;
// MCD12Q1 v6.1 は 2001–2024 年まで（2025 はまだ未提供）
var END_YEAR   = 2024;

var YEARS = ee.List.sequence(START_YEAR, END_YEAR);

// IGBP クラスコードと名前（JS のふつうの配列）
var classCodes = [
  0,   // Water
  1,   // Evergreen Needleleaf Forest
  2,   // Evergreen Broadleaf Forest
  3,   // Deciduous Needleleaf Forest
  4,   // Deciduous Broadleaf Forest
  5,   // Mixed Forest
  6,   // Closed Shrublands
  7,   // Open Shrublands
  8,   // Woody Savannas
  9,   // Savannas
  10,  // Grasslands
  11,  // Permanent Wetlands
  12,  // Croplands
  13,  // Urban and Built-up
  14,  // Cropland/Natural Vegetation Mosaic
  15,  // Snow and Ice
  16   // Barren or Sparsely Vegetated
];

var classNames = [
  'water',
  'evergreen_needle',
  'evergreen_broadleaf',
  'deciduous_needle',
  'deciduous_broadleaf',
  'mixed_forest',
  'closed_shrub',
  'open_shrub',
  'woody_savanna',
  'savanna',
  'grassland',
  'wetland',
  'cropland',
  'urban',
  'cropland_natural_mosaic',
  'snow_ice',
  'barren'
];

// -----------------------------
// 1. インドネシア ADM2（GAUL 2015, simplified 500m）
// -----------------------------
var adm2 = ee.FeatureCollection('FAO/GAUL_SIMPLIFIED_500m/2015/level2')
  .filter(ee.Filter.eq('ADM0_NAME', 'Indonesia'))
  .select(
    ['ADM1_NAME', 'ADM2_CODE', 'ADM2_NAME'],
    ['adm1_name', 'adm2_code', 'adm2_name']
  );

print('ADM2 count:', adm2.size());
Map.addLayer(adm2, {}, 'IDN ADM2');
Map.centerObject(adm2, 5);

// -----------------------------
// 2. 年ループで MODIS LC を ADM2 に集計
// -----------------------------
var allYearsFc = ee.FeatureCollection(
  YEARS.map(function(y) {
    y = ee.Number(y);

    // 2-1. 指定年の MODIS LC_Type1 (IGBP)
    var lc = ee.ImageCollection('MODIS/061/MCD12Q1')
      .filter(ee.Filter.calendarRange(y, y, 'year'))
      .first()
      .select('LC_Type1');

    print('LC_Type1, year =', y, lc);

    // 2-2. ピクセル面積 (ha)
    var areaHa = ee.Image.pixelArea()
      .divide(10000)            // m2 → ha
      .rename('total_ha');

    // 2-3. クラス別の面積バンドを作成（JS の for ループでバンド名を決める）
    var classStack = ee.Image([]);   // バンドなし Image

    for (var i = 0; i < classCodes.length; i++) {
      var code = classCodes[i];
      var name = classNames[i];  // ふつうの文字列

      var band = areaHa
        .updateMask(lc.eq(code))
        .rename('ha_' + name);   // 'ha_water', 'ha_cropland' など

      classStack = classStack.addBands(band);
    }

    // total_ha + 各クラスの ha_* バンドをスタック
    var stack = areaHa.addBands(classStack);

    // 2-4. ADM2 に reduceRegions で合計
    var stats = stack.reduceRegions({
      collection: adm2,
      reducer: ee.Reducer.sum(),
      scale: 500,
      crs: 'EPSG:4326',
      tileScale: 4
    });

    // 2-5. クラス別の share_* を計算して year を付与
    stats = stats.map(function(f) {
      f = ee.Feature(f);
      var total = ee.Number(f.get('total_ha'));

      // 各クラスについて share_* を計算
      for (var j = 0; j < classNames.length; j++) {
        var cname     = classNames[j];
        var haName    = 'ha_' + cname;
        var shareName = 'share_' + cname;

        var haVal = ee.Number(f.get(haName));
        var share = ee.Algorithms.If(
          total.gt(0),
          haVal.divide(total),
          null
        );

        f = f.set(shareName, share);
      }

      return f.set('year', y);
    });

    return stats;
  })
).flatten();

print('Sample (all years, all classes):', allYearsFc.limit(10));

// -----------------------------
// 3. Google Drive に CSV として出力
// -----------------------------
Export.table.toDrive({
  collection: allYearsFc,
  description: 'idn_modis_lc_allclasses_adm2_2019_2024',
  fileNamePrefix: 'idn_modis_lc_allclasses_adm2_2019_2024',
  fileFormat: 'CSV'
});

