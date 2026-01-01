/***** パラメータ *****/
var YEARS = ee.List.sequence(2019, 2024);   // 主分析の年
var TREECOVER_THRESHOLD = 30;
var SCALE = 60;     // まず60mで通す。最終版だけ30mに戻してOK
var TILE  = 8;

/***** 境界（GAUL ADM2, 簡略化）*****/
var ADM2 = ee.FeatureCollection('FAO/GAUL/2015/level2')
  .filter(ee.Filter.eq('ADM0_NAME','Indonesia'))
  .map(function(f){ return f.simplify(100); });

/* 島ごとに分割（必要に応じて名称調整。print(ADM2.limit(1))で確認可） */
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

/***** GFC 読み込み・マスク *****/
var gfc      = ee.Image('UMD/hansen/global_forest_change_2024_v1_12');
var lossyear = gfc.select('lossyear');                       // 1→2001,…,24→2024
var tree2000 = gfc.select('treecover2000');
var land     = gfc.select('datamask').eq(1);
var forestMask = tree2000.gte(TREECOVER_THRESHOLD).and(land);
var pixelAreaHa = ee.Image.pixelArea().divide(10000);

/***** サブセットを受けて、年リストを処理→Export する関数 *****/
function exportShard(tag, provinces, years){
  // 1) サブ境界とラスタID
  var subADM2 = ADM2.filter(ee.Filter.inList('ADM1_NAME', ee.List(provinces)));
  var geom    = subADM2.geometry();
  var adm2Raster = subADM2.reduceToImage({
    properties: ['ADM2_CODE'],
    reducer: ee.Reducer.first()
  }).rename('adm2_id');

  // 2) ベース面積（2000年自然林）を一度だけ group 集計
  var baseImg = pixelAreaHa.updateMask(forestMask).clip(geom);
  var baseGroups = baseImg.addBands(adm2Raster).reduceRegion({
    reducer: ee.Reducer.sum().group({groupField: 1, groupName: 'ADM2_CODE'}),
    geometry: geom, scale: SCALE, tileScale: TILE, bestEffort: true, maxPixels: 1e13
  }).get('groups');

  var baseFC = ee.FeatureCollection(ee.List(baseGroups).map(function(d){
    d = ee.Dictionary(d);
    return ee.Feature(null, {
      ADM2_CODE: d.get('ADM2_CODE'),
      forest2000_ha: d.getNumber('sum')
    });
  }));

  var meta = subADM2.select(['ADM2_CODE','ADM2_NAME','ADM1_NAME']);

  // 3) 年ごとの loss_ha を group 集計（年ごとに処理：省メモリ）
  function lossByYear(y){
    y = ee.Number(y);
    var lossMask = lossyear.eq(y.subtract(2000)).and(forestMask);
    var lossImg  = pixelAreaHa.updateMask(lossMask).clip(geom);

    var groups = lossImg.addBands(adm2Raster).reduceRegion({
      reducer: ee.Reducer.sum().group({groupField: 1, groupName: 'ADM2_CODE'}),
      geometry: geom, scale: SCALE, tileScale: TILE, bestEffort: true, maxPixels: 1e13
    }).get('groups');

    return ee.FeatureCollection(ee.List(groups).map(function(d){
      d = ee.Dictionary(d);
      return ee.Feature(null, {
        ADM2_CODE: d.get('ADM2_CODE'),
        year: y,
        loss_ha: d.getNumber('sum')
      });
    }));
  }
  var lossLong = ee.FeatureCollection(years.map(lossByYear)).flatten();

  // 4) Join（県名・ベース面積を付与）
  var withMeta = ee.Join.saveFirst('match').apply(
    lossLong, meta,
    ee.Filter.equals({leftField: 'ADM2_CODE', rightField: 'ADM2_CODE'})
  );
  var withBase = ee.Join.saveFirst('bmatch').apply(
    withMeta, baseFC,
    ee.Filter.equals({leftField: 'ADM2_CODE', rightField: 'ADM2_CODE'})
  );

  var final = ee.FeatureCollection(withBase.map(function(f){
    var m = ee.Feature(f.get('match'));
    var b = ee.Feature(f.get('bmatch'));
    var baseHa = ee.Number(b.get('forest2000_ha'));
    var lossHa = ee.Number(f.get('loss_ha'));
    var rate   = ee.Algorithms.If(baseHa.gt(0), lossHa.divide(baseHa), null);
    return ee.Feature(null, {
      ADM1_NAME: m.get('ADM1_NAME'),
      ADM2_CODE: f.get('ADM2_CODE'),
      ADM2_NAME: m.get('ADM2_NAME'),
      year:      f.get('year'),
      loss_ha:   lossHa,
      forest2000_ha: baseHa,
      defor_rate: rate
    });
  }));

  // 5) 出力（Consoleにprintしない）
  Export.table.toDrive({
    collection: final,
    description: 'IDN_GFC_loss_ADM2_'+tag+'_tc'+TREECOVER_THRESHOLD,
    fileNamePrefix: 'idn_gfc_loss_adm2_'+tag+'_tc'+TREECOVER_THRESHOLD,
    fileFormat: 'CSV'
  });
}

/***** 実行：年もさらに2分割して安全運転 *****/
var YEARS_A = ee.List.sequence(2019, 2021);
var YEARS_B = ee.List.sequence(2022, 2024);

Object.keys(GROUPS).forEach(function(k){
  exportShard(k+'_2019_2021', GROUPS[k], YEARS_A);
  exportShard(k+'_2022_2024', GROUPS[k], YEARS_B);
});

// 表示は軽い確認だけ（任意）
Map.centerObject(ADM2, 5);
Map.addLayer(lossyear.eq(24).and(forestMask), {}, 'Loss 2024 mask');

