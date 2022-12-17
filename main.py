import os
import requests


if __name__ == '__main__':
    # 토픽 생성
    os.system('docker exec broker kafka-topics --bootstrap-server localhost:9092 --create --topic USERPROFILE --partitions 1')
    os.system('docker exec broker kafka-topics --bootstrap-server localhost:9092 --create --topic COUNTRY-CSV --partitions 1')

    # 스크립트 이동
    os.system('docker cp ./create_country_table.ksql ksqldb-cli:/')
    os.system('docker cp ./create_user_profile.ksql ksqldb-cli:/')
    os.system('docker cp ./user_profile_pretty.ksql ksqldb-cli:/')

    #생성
    """
    run script /create_user_profile.ksql;
    run script /user_profile_pretty.ksql;
    run script /create_country_table.ksql;
    """
