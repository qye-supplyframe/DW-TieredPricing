
jar="get_price_data-*-jar-with-dependencies.jar"
spark3-submit \
--class com.supplyframe.ds.tieredPricing.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$jar