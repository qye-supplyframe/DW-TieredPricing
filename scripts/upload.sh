mvn clean package
scp target/get_price_data-*-jar-with-dependencies.jar us-lax-9a-ym-02.vpc.supplyframe.com:/home/qye/price_estimation_real_time
scp scripts/job.sh us-lax-9a-ym-02.vpc.supplyframe.com:/home/qye/price_estimation_real_time