
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_UPDATED_QHAWAX = BASE_URL_QAIRA + '/api/QhawaxFondecyt/'

if __name__ == '__main__':
	all_qhawax_station = json.loads(requests.get(GET_UPDATED_QHAWAX).text)