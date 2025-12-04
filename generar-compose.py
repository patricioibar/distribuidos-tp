import enum
import sys


MIDDLEWARE_ADDRESS = "amqp://guest:guest@rabbitmq:5672/"

class FilterType(enum.Enum):
    TbyYear = "TbyYear"
    TbyHour = "TbyHour"
    TbyAmount = "TbyAmount"
    TIbyYear = "TIbyYear"

def add_filter_service(nodes: int, filterType: FilterType, input_name: str, output_name: str, monitors_count: int) -> str:
  monitor_deps = "\n".join(
    [f"      monitor-{n}:\n        condition: service_started"
     for n in range(1, monitors_count + 1)]
  )

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
      MONITORS_COUNT: {monitors_count}
    depends_on:
      rabbitmq:
        condition: service_healthy
{monitor_deps}
    networks:
      - coffee_analysis_net
'''
        result += filter_template

    
  return result

def add_itemsAggregator_service(nodes: int, monitors_count: int) -> str:
  monitor_deps = "\n".join(
    [f"      monitor-{n}:\n        condition: service_started"
     for n in range(1, monitors_count + 1)]
  )

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
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
        result += aggregator_template
  return result

def add_itemsReducer_service(nodes: int, monitors_count: int) -> str:
  monitor_deps = "\n".join(
    [f"      monitor-{n}:\n        condition: service_started"
     for n in range(1, monitors_count + 1)]
  )
  reducer_template = r'''
  items-reducer:
    image: aggregator:latest
    container_name: items-reducer
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "items-reducer"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"year-month\",\"item_id\"]"
      AGGREGATIONS: "[{\"col\":\"sum_subtotal\",\"func\":\"sum\"},{\"col\":\"sum_quantity\",\"func\":\"sum\"}]"
      QUERY_NAME: "items"
      INPUT_NAME: "items-reducer_input"
      OUTPUT_NAME: "items-reducer_output"
      LOG_LEVEL: "INFO"
      RETAININGS: "[{\"amount-retained\":1,\"group-by\":\"year-month\",\"value\":\"sum_subtotal\",\"largest\":true}, {\"amount-retained\":1,\"group-by\":\"year-month\",\"value\":\"sum_quantity\",\"largest\":true}]"
      OUTPUT_BATCH_SIZE: 50
      IS_REDUCER: "true"
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
  return reducer_template

def add_itemNames_joiner(monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"year-month\",\"item_name\",\"sum_subtotal\",\"sum_quantity\"]"
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
    return joiner_template

def add_tpvAggregator_service(nodes: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
        result += aggregator_template
    return result

def add_tpvReducer_service(nodes: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
    reducer_template = r'''
  tpv-reducer:
    image: aggregator:latest
    container_name: tpv-reducer
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "tpv-reducer"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"year\",\"semester\",\"store_id\"]"
      AGGREGATIONS: "[{\"col\":\"sum_final_amount\",\"func\":\"sum\"}]"
      QUERY_NAME: "tpv"
      INPUT_NAME: "tpv-reducer_input"
      OUTPUT_NAME: "tpv-reducer_output"
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      IS_REDUCER: "true"
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
    return reducer_template

def add_tpv_joiner(nodes: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
        result += joiner_template
    return result

def add_topUserAggregator_service(nodes: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
        result += aggregator_template
    return result

def add_topUserReducer_service(nodes: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
    reducer_template = r'''
  topuser-reducer:
    image: aggregator:latest
    container_name: topuser-reducer
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      WORKER_ID: "topuser-reducer"
      WORKERS_COUNT: ''' + str(nodes) + r'''
      MIDDLEWARE_ADDRESS: "''' + MIDDLEWARE_ADDRESS + r'''"
      GROUP_BY: "[\"store_id\",\"user_id\"]"
      AGGREGATIONS: "[{\"col\":\"count_transaction_id\",\"func\":\"sum\"}]"
      QUERY_NAME: "topuser"
      INPUT_NAME: "topuser-reducer_input"
      OUTPUT_NAME: "topuser-reducer_output"
      LOG_LEVEL: "INFO"
      RETAININGS: "[{\"amount-retained\":3,\"group-by\":\"store_id\",\"value\":\"count_transaction_id\",\"largest\":true}]"
      OUTPUT_BATCH_SIZE: 3
      IS_REDUCER: "true"
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
    return reducer_template


def add_topUserBirthdate_joiner(nodes: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"store_id\",\"birthdate\",\"count_transaction_id\"]"
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
    networks:
      - coffee_analysis_net
'''
        result += joiner_template
    return result

def add_topUserStoreName_joiner(monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      LOG_LEVEL: "INFO"
      OUTPUT_BATCH_SIZE: 200
      OUTPUT_COLUMNS: "[\"store_name\",\"birthdate\",\"count_transaction_id\"]"
      MONITORS_COUNT: ''' + str(monitors_count) + r'''
    depends_on:
      rabbitmq:
        condition: service_healthy
''' + monitor_deps + r'''
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
        - subnet: 172.28.0.0/16
'''
    return volumes_and_networks

def add_coffeeAnalyzer_service(total_workers: int, monitors_count: int) -> str:
    monitor_deps = "\n".join(
        [f"      monitor-{n}:\n        condition: service_started"
         for n in range(1, monitors_count + 1)]
    )
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
      MONITORS_COUNT: {monitors_count}
    depends_on:
      rabbitmq:
        condition: service_healthy
{monitor_deps}
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "30001"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - coffee_analysis_net
'''
    return coffee_analyzer_template

def add_monitors(num_monitors: int) -> str:
  result = ""
  for i in range(1, num_monitors + 1):
    monitor_template = f'''
  monitor-{i}:
    image: monitor:latest
    container_name: monitor-{i}
    build:
      context: .
      dockerfile: ./monitor/Dockerfile
    environment:
      MONITOR_ID: monitor-{i}
      MONITORS_COUNT: {num_monitors}
      PORT: 9000
    depends_on:
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "30001"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - coffee_analysis_net
'''
    result += monitor_template
  return result

def add_analyst_service(nodes: int) -> str:
    result = ""
    for i in range(1, nodes + 1):
        analyst_template = f'''
  analyst-{i}:
    image: analyst:latest
    container_name: analyst-{i}
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
        result += analyst_template
    return result

def generate_compose_file(fileName: str, nodes_count: dict):
    num_filter_years = nodes_count.get("filter-TbyYear", 1)
    num_filter_hours = nodes_count.get("filter-TbyHour", 1)
    num_filter_amount = nodes_count.get("filter-TbyAmount", 1)
    num_filter_items = nodes_count.get("filter-TIbyYear", 1)
    num_items_aggregators = nodes_count.get("items-aggregator", 1)
    num_tpv_aggregators = nodes_count.get("tpv-aggregator", 1)
    num_tpv_joiners = nodes_count.get("tpv-joiner", 1)
    num_topuser_aggregators = nodes_count.get("topuser-aggregator", 1)
    num_topuser_birthdate_joiners = nodes_count.get("topuser-birthdate-joiner", 1)

    num_analysts = nodes_count.get("analyst", 1)
    num_monitors = nodes_count.get("monitor", 1)

    total_workers = (
        num_filter_years +
        num_filter_hours +
        num_filter_amount +
        num_filter_items +
        num_items_aggregators +
        num_tpv_aggregators +
        num_tpv_joiners +
        num_topuser_aggregators +
        num_topuser_birthdate_joiners +
        5  # For reducers and other single services
    )

    compose_content = f'''
services:
{add_rabbitmq_service()}
{add_coffeeAnalyzer_service(total_workers, num_monitors)}
{add_analyst_service(num_analysts)}
{add_filter_service(num_filter_years, FilterType.TbyYear, "transactions", "filtered-transactions-year", num_monitors)}
{add_filter_service(num_filter_hours, FilterType.TbyHour, "filtered-transactions-year", "filtered-transactions-yearhour", num_monitors)}
{add_filter_service(num_filter_amount, FilterType.TbyAmount, "filtered-transactions-yearhour", "query1_sink", num_monitors)}
{add_filter_service(num_filter_items, FilterType.TIbyYear, "transaction_items", "filtered-years-items", num_monitors)}
{add_itemsAggregator_service(num_items_aggregators, num_monitors)}
{add_itemsReducer_service(num_items_aggregators, num_monitors)}
{add_itemNames_joiner(num_monitors)}
{add_tpvAggregator_service(num_tpv_aggregators, num_monitors)}
{add_tpvReducer_service(num_tpv_aggregators, num_monitors)}
{add_tpv_joiner(num_tpv_joiners, num_monitors)}
{add_topUserAggregator_service(num_topuser_aggregators, num_monitors)}
{add_topUserReducer_service(num_topuser_aggregators, num_monitors)}
{add_topUserBirthdate_joiner(num_topuser_birthdate_joiners, num_monitors)}
{add_topUserStoreName_joiner(num_monitors)}
{add_monitors(num_monitors)}
{add_volumes_networks()}
'''

    with open(fileName, "w") as f:
        f.write(compose_content)

nodes_count = {
    "filter-TbyYear": 3,
    "filter-TbyHour": 3,
    "filter-TbyAmount": 3,
    "filter-TIbyYear": 3,
    "items-aggregator": 3,
    "tpv-aggregator": 3,
    "tpv-joiner": 3,
    "topuser-aggregator": 3,
    "topuser-birthdate-joiner": 3,
    "analyst": 1,
    "monitor": 3,
}

if __name__ == "__main__":
  file_name = "docker-compose.yml"
  print("Generating docker-compose file...")
  if len(sys.argv) > 1:
    file_name = sys.argv[1]
  
  generate_compose_file(file_name, nodes_count)