# Aggregate data, report median value

Приложение читает предопределенный .jsonl файл с помощью **PySpark**, проводит необходимые вычисления для расчета медианы значений поля "**value**" для каждого сенсора в разрезе дня, записывает результат вычисления в .json файл "**Result.json**" в корне приложения.

Перед запуском приложения необходимо установить требуемые зависимости:

```console
python3 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Результат выполнения скрипта:
```json lines
{"date":"2019-01-01","input":"sensor_1","median_value":-1.5093470081582478}
{"date":"2019-01-01","input":"sensor_2","median_value":49.27594380832702}
{"date":"2019-01-01","input":"sensor_3","median_value":-27.20107668693611}
{"date":"2019-01-01","input":"sensor_4","median_value":63.93840751108315}
{"date":"2019-01-01","input":"sensor_5","median_value":2.2164521969016606}
{"date":"2019-01-01","input":"sensor_6","median_value":-14.486821610380595}
{"date":"2019-01-01","input":"sensor_7","median_value":6.505575648497579}
{"date":"2019-01-01","input":"sensor_8","median_value":2.856015265934839}
{"date":"2019-01-01","input":"sensor_9","median_value":28.414813514103134}
{"date":"2019-01-01","input":"sensor_10","median_value":56.734940424934855}
{"date":"2019-01-01","input":"sensor_11","median_value":-27.300326546044936}
{"date":"2019-01-01","input":"sensor_12","median_value":0.08863624315569074}
{"date":"2019-01-01","input":"sensor_13","median_value":5.41652291324601}
{"date":"2019-01-01","input":"sensor_14","median_value":24.162046048556213}
{"date":"2019-01-01","input":"sensor_15","median_value":-61.255251603892056}
{"date":"2019-01-01","input":"sensor_16","median_value":-50.18858078740446}
{"date":"2019-01-02","input":"sensor_1","median_value":0.5425796106877877}
{"date":"2019-01-02","input":"sensor_2","median_value":-24.76242516778732}
{"date":"2019-01-02","input":"sensor_3","median_value":-5.379869654533433}
{"date":"2019-01-02","input":"sensor_4","median_value":-12.857595687047926}
{"date":"2019-01-02","input":"sensor_5","median_value":-21.61275724728727}
{"date":"2019-01-02","input":"sensor_6","median_value":-3.789842213017898}
{"date":"2019-01-02","input":"sensor_7","median_value":-9.992178403706797}
{"date":"2019-01-02","input":"sensor_8","median_value":1.1246916961856883}
{"date":"2019-01-02","input":"sensor_9","median_value":3.812176936148213}
{"date":"2019-01-02","input":"sensor_10","median_value":-3.2429011900463394}
{"date":"2019-01-02","input":"sensor_11","median_value":2.1911265587555535}
{"date":"2019-01-02","input":"sensor_12","median_value":52.977064550704036}
{"date":"2019-01-02","input":"sensor_13","median_value":1.647111780489098}
{"date":"2019-01-02","input":"sensor_14","median_value":-6.127996051400741}
{"date":"2019-01-02","input":"sensor_15","median_value":13.24150628249624}
{"date":"2019-01-02","input":"sensor_16","median_value":-27.08997575528481}
{"date":"2019-01-03","input":"sensor_1","median_value":32.33559701195404}
{"date":"2019-01-03","input":"sensor_2","median_value":-13.131649698255657}
{"date":"2019-01-03","input":"sensor_3","median_value":-7.1931601694060445}
{"date":"2019-01-03","input":"sensor_4","median_value":-9.240635818951372}
{"date":"2019-01-03","input":"sensor_5","median_value":-13.963925312038313}
{"date":"2019-01-03","input":"sensor_6","median_value":-17.4168782411543}
{"date":"2019-01-03","input":"sensor_7","median_value":-25.0422866576233}
{"date":"2019-01-03","input":"sensor_8","median_value":16.27074840037003}
{"date":"2019-01-03","input":"sensor_9","median_value":35.623901418221294}
{"date":"2019-01-03","input":"sensor_10","median_value":-23.167715860732905}
{"date":"2019-01-03","input":"sensor_11","median_value":26.16144422331207}
{"date":"2019-01-03","input":"sensor_12","median_value":-9.380315546124674}
{"date":"2019-01-03","input":"sensor_13","median_value":0.8492545599647106}
{"date":"2019-01-03","input":"sensor_14","median_value":-9.582107085239226}
{"date":"2019-01-03","input":"sensor_15","median_value":14.77210093742434}
{"date":"2019-01-03","input":"sensor_16","median_value":-47.03623722473375}
{"date":"2019-01-04","input":"sensor_1","median_value":1.7866127741297764}
{"date":"2019-01-04","input":"sensor_2","median_value":-14.814614842930803}
{"date":"2019-01-04","input":"sensor_3","median_value":-45.02120286555589}
{"date":"2019-01-04","input":"sensor_4","median_value":30.227981688859952}
{"date":"2019-01-04","input":"sensor_5","median_value":-66.06070726001482}
{"date":"2019-01-04","input":"sensor_6","median_value":36.50404389610479}
{"date":"2019-01-04","input":"sensor_7","median_value":9.34376983828799}
{"date":"2019-01-04","input":"sensor_8","median_value":5.114904864518577}
{"date":"2019-01-04","input":"sensor_9","median_value":-18.178347435967517}
{"date":"2019-01-04","input":"sensor_10","median_value":30.88600674721182}
{"date":"2019-01-04","input":"sensor_11","median_value":41.2747852779868}
{"date":"2019-01-04","input":"sensor_12","median_value":-25.007609441182076}
{"date":"2019-01-04","input":"sensor_13","median_value":20.108732617512786}
{"date":"2019-01-04","input":"sensor_14","median_value":1.2431540888246402}
{"date":"2019-01-04","input":"sensor_15","median_value":22.500472059146567}
{"date":"2019-01-04","input":"sensor_16","median_value":-20.846406077413757}
```