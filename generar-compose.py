

import enum


MIDDLEWARE_ADDRESS = "amqp://guest:guest@rabbitmq:5672/"

class FilterType(enum.Enum):
    TbyYear = "TbyYear"
    TbyHour = "TbyHour"
    TbyAmount = "TbyAmount"
    TIbyYear = "TIbyYear"

def add_filter_service(nodes: int, filterType: FilterType, input_name: str, output_name: str) -> str:
    result = ""
    filterType = filterType.value
    for i in range(1, nodes + 1):
        filter_template = f'''
  filter-{filterType}-{i}:
    image: filter:latest
    build:
      context: .
      dockerfile: filter/Dockerfile
    container_name: filter-{filterType}-{i}
    environment:
      LOG_LEVEL: "INFO"
      FILTER_ID: "filter-{filterType}-{i}"
      WORKERS_COUNT: "{nodes}"
      FILTER_TYPE: "{filterType}"
      CONSUMER_NAME: "transactions-{filterType}-consumer"
      SOURCE_QUEUE: "{input_name}"
      OUTPUT_EXCHANGE: "{output_name}"
      MW_ADDRESS: "{MIDDLEWARE_ADDRESS}"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
        result += filter_template
    return result

def add_itemsAggregator_service(nodes: int) -> str:
    result = ""
    for i in range(1, nodes + 1):
        aggregator_template = r'''
  items-aggregator-''' + str(i) + r''':
    image: aggregator:latest
    container_name: items-aggregator-''' + str(i) + r'''
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "items-aggregator-''' + str(i) + r'''"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"year-month\",\"item_id\"]"
      AGGREGATIONS: "[{\"col\":\"subtotal\",\"func\":\"sum\"},{\"col\":\"quantity\",\"func\":\"sum\"}]"
      QUERY_NAME: "items"
      INPUT_NAME: "filtered-years-items"
      OUTPUT_NAME: "items-reducer_input"
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      IS_REDUCER: "false"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
        result += aggregator_template
    return result

def add_itemsReducer_service() -> str:
    reducer_template = r'''
  items-reducer:
    image: aggregator:latest
    container_name: items-reducer
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "items-reducer"
      WORKERS_COUNT: 1
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"year-month\",\"item_id\"]"
      AGGREGATIONS: "[{\"col\":\"sum_subtotal\",\"func\":\"sum\"},{\"col\":\"sum_quantity\",\"func\":\"sum\"}]"
      QUERY_NAME: "items"
      INPUT_NAME: "items-reducer_input"
      OUTPUT_NAME: "items-reducer_output"
      LOG_LEVEL: "DEBUG"
      RETAININGS: "[{\"amount-retained\":1,\"group-by\":\"year-month\",\"value\":\"sum_subtotal\",\"largest\":true}, {\"amount-retained\":1,\"group-by\":\"year-month\",\"value\":\"sum_quantity\",\"largest\":true}]"
      OUTPUT_BATCH_SIZE: 50
      IS_REDUCER: "true"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
    return reducer_template

def add_itemNames_joiner() -> str:
    joiner_template = r'''
  items-names-joiner:
    image: joiner:latest
    container_name: items-names-joiner
    build:
      context: .
      dockerfile: joiner/Dockerfile
    environment:
      WORKER_ID: "items-names-joiner"
      WORKERS_COUNT: 1
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      JOIN_KEY: "item_id"
      QUERY_NAME: "items"
      RIGHT_INPUT_NAME: "items-reducer_output"
      LEFT_INPUT_NAME: "menu_items"
      OUTPUT_NAME: "query2_sink"
      LOG_LEVEL: "DEBUG"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"year-month\",\"item_name\",\"subtotal_or_quantity\"]"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
    return joiner_template

def add_tpvAggregator_service(nodes: int) -> str:
    result = ""
    for i in range(1, nodes + 1):
        aggregator_template = r'''
  tpv-aggregator-''' + str(i) + r''':
    image: aggregator:latest
    container_name: tpv-aggregator-''' + str(i) + r'''
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "tpv-aggregator-''' + str(i) + r'''"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"year\",\"semester\",\"store_id\"]"
      AGGREGATIONS: "[{\"col\":\"final_amount\",\"func\":\"sum\"}]"
      QUERY_NAME: "tpv"
      INPUT_NAME: "filtered-transactions-yearhour"
      OUTPUT_NAME: "tpv-reducer_input"
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      IS_REDUCER: "false"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
        result += aggregator_template
    return result

def add_tpvReducer_service() -> str:
    reducer_template = r'''
  tpv-reducer:
    image: aggregator:latest
    container_name: tpv-reducer
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "tpv-reducer"
      WORKERS_COUNT: 1
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"year\",\"semester\",\"store_id\"]"
      AGGREGATIONS: "[{\"col\":\"sum_final_amount\",\"func\":\"sum\"}]"
      QUERY_NAME: "tpv"
      INPUT_NAME: "tpv-reducer_input"
      OUTPUT_NAME: "tpv-reducer_output"
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      IS_REDUCER: "true"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
    return reducer_template

def add_tpv_joiner(nodes: int) -> str:
    result = ""
    for i in range(1, nodes + 1):
        joiner_template = r'''
  tpv-joiner-''' + str(i) + r''':
    image: joiner:latest
    container_name: tpv-joiner-''' + str(i) + r'''
    build:
      context: .
      dockerfile: joiner/Dockerfile
    environment:
      WORKER_ID: "tpv-joiner-''' + str(i) + r'''"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      JOIN_KEY: "store_id"
      QUERY_NAME: "tpv"
      RIGHT_INPUT_NAME: "stores"
      LEFT_INPUT_NAME: "tpv-reducer_output"
      OUTPUT_NAME: "query3_sink"
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"year\",\"semester\",\"store_name\",\"sum_final_amount\"]"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
        result += joiner_template
    return result

def add_topUserAggregator_service(nodes: int) -> str:
    result = ""
    for i in range(1, nodes + 1):
        aggregator_template = r'''
  topuser-aggregator-''' + str(i) + r''':
    image: aggregator:latest
    container_name: topuser-aggregator-''' + str(i) + r'''
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "topuser-aggregator-''' + str(i) + r'''"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"store_id\",\"user_id\"]"
      AGGREGATIONS: "[{\"col\":\"transaction_id\",\"func\":\"count\"}]"
      QUERY_NAME: "topuser"
      INPUT_NAME: "filtered-transactions-year"
      OUTPUT_NAME: "topuser-reducer_input"
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      IS_REDUCER: "false"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
        result += aggregator_template
    return result

def add_topUserReducer_service() -> str:
    reducer_template = r'''
  topuser-reducer:
    image: aggregator:latest
    container_name: topuser-reducer
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "topuser-reducer"
      WORKERS_COUNT: 1
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"store_id\",\"user_id\"]"
      AGGREGATIONS: "[{\"col\":\"count_transaction_id\",\"func\":\"sum\"}]"
      QUERY_NAME: "topuser"
      INPUT_NAME: "topuser-reducer_input"
      OUTPUT_NAME: "topuser-reducer_output"
      LOG_LEVEL: "DEBUG"
      RETAININGS: "[{\"amount-retained\":3,\"group-by\":\"store_id\",\"value\":\"count_transaction_id\",\"largest\":true}]"
      OUTPUT_BATCH_SIZE: 3
      IS_REDUCER: "true"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
    return reducer_template


def add_topUserBirthdate_joiner(nodes: int) -> str:
    result = ""
    for i in range(1, nodes + 1):
        joiner_template = r'''
  topuser-birthdate-joiner-''' + str(i) + r''':
    image: joiner:latest
    container_name: topuser-birthdate-joiner-''' + str(i) + r'''
    build:
      context: .
      dockerfile: joiner/Dockerfile
    environment:
      WORKER_ID: "topuser-birthdate-joiner-''' + str(i) + r'''"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      JOIN_KEY: "user_id"
      QUERY_NAME: "topuser"
      RIGHT_INPUT_NAME: "topuser-reducer_output"
      LEFT_INPUT_NAME: "users"
      OUTPUT_NAME: "topuser-birthdate-joiner_output"
      LOG_LEVEL: "DEBUG"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"store_id\",\"birthdate\",\"count_transaction_id\"]"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
        result += joiner_template
    return result

def add_topUserStoreName_joiner() -> str:
    joiner_template = r'''
  topuser-storename-joiner:
    image: joiner:latest
    container_name: topuser-storename-joiner
    build:
      context: .
      dockerfile: joiner/Dockerfile
    environment:
      WORKER_ID: "topuser-storename-joiner"
      WORKERS_COUNT: 1
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      JOIN_KEY: "store_id"
      QUERY_NAME: "topuser"
      RIGHT_INPUT_NAME: "topuser-birthdate-joiner_output"
      LEFT_INPUT_NAME: "stores"
      OUTPUT_NAME: "query4_sink"
      LOG_LEVEL: "DEBUG"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"store_name\",\"birthdate\",\"count_transaction_id\"]"
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
    return joiner_template

def add_rabbitmq_service() -> str:
    rabbitmq_template = '''
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    ports:
      - "5672:5672" # AMQP port
      - "15672:15672" # Management UI port
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq # Persistent data
      - rabbitmq_logs:/var/log/rabbitmq # Persistent logs
    healthcheck:
      test: ["CMD", "rabbitmqctl", "status"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - coffee_analysis_net
'''
    return rabbitmq_template

def add_volumes_networks() -> str:
    volumes_and_networks = '''
volumes:
  rabbitmq_data:
  rabbitmq_logs:

networks:
  coffee_analysis_net:
    ipam:
      driver: default
      config:
        - subnet: 172.18.0.0/16
'''
    return volumes_and_networks

def add_coffeeAnalyzer_service(total_workers: int) -> str:
    coffee_analyzer_template = f'''
  coffee-analyzer:
    image: coffee-analyzer:latest
    container_name: coffee-analyzer
    build:
      context: .
      dockerfile: ./coffee-analyzer/Dockerfile
    volumes:
      - ./coffee-analyzer/config.json:/app/config.json
    environment:
      TOTAL_WORKERS: {total_workers}
    depends_on:
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "30001"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - coffee_analysis_net
'''
    return coffee_analyzer_template

def add_analyst_service() -> str:
    analyst_template = '''
  analyst:
    image: analyst:latest
    container_name: analyst
    build:
      context: .
      dockerfile: ./analyst/Dockerfile
    volumes:
      - ./analyst/config.json:/app/config.json
      - ./analyst/.data/:/app/.data/
      - ./analyst/results/:/app/results/
    depends_on:
      coffee-analyzer:
        condition: service_healthy
    networks:
      - coffee_analysis_net
'''
    return analyst_template

def generate_compose_file(fileName: str):
    num_filter_years = 3
    num_filter_hours = 3
    num_filter_amount = 2
    num_filter_items = 3
    num_items_aggregators = 3
    num_tpv_aggregators = 3
    num_tpv_joiners = 3
    num_topuser_aggregators = 3
    num_topuser_birthdate_joiners = 5
    
    total_workers = (
        num_filter_years +
        num_filter_hours +
        num_filter_amount +
        num_filter_items +
        num_items_aggregators +
        num_tpv_aggregators +
        num_tpv_joiners +
        num_topuser_aggregators +
        num_topuser_birthdate_joiners
    )

    compose_content = f'''
services:
{add_rabbitmq_service()}
{add_coffeeAnalyzer_service(total_workers)}
{add_analyst_service()}
{add_filter_service(num_filter_years, FilterType.TbyYear, "transactions", "filtered-transactions-year")}
{add_filter_service(num_filter_hours, FilterType.TbyHour, "filtered-transactions-year", "filtered-transactions-yearhour")}
{add_filter_service(num_filter_amount, FilterType.TbyAmount, "filtered-transactions-yearhour", "query1_sink")}
{add_filter_service(num_filter_items, FilterType.TIbyYear, "transaction_items", "filtered-years-items")}
{add_itemsAggregator_service(num_items_aggregators)}
{add_itemsReducer_service()}
{add_itemNames_joiner()}
{add_tpvAggregator_service(num_tpv_aggregators)}
{add_tpvReducer_service()}
{add_tpv_joiner(num_tpv_joiners)}
{add_topUserAggregator_service(num_topuser_aggregators)}
{add_topUserReducer_service()}
{add_topUserBirthdate_joiner(num_topuser_birthdate_joiners)}
{add_topUserStoreName_joiner()}
{add_volumes_networks()}
'''

    with open(fileName, "w") as f:
        f.write(compose_content)

if __name__ == "__main__":
    generate_compose_file("docker-compose_test.yml")