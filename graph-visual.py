import pygal
import json

with open('data/graph.json/part-00000') as f:
  data = json.load(f)

def get_labels(obj):
  return obj['Percentage']

def get_employed_mean(obj):
  return obj['EmployedMean']

def get_employed_variance(obj):
  return obj['EmployedVariance']

def get_unemployed_mean(obj):
  return obj['UnEmployedMean']

def get_unemployed_variance(obj):
  return obj['UnEmployedVariance']

values = sorted(data['data']['values'], key=get_labels)

line_chart = pygal.Line()
line_chart.title = "Bootstrap App: Employment"
line_chart.x_labels = map(get_labels, values)

line_chart.add('Employed Mean', list(map(get_employed_mean, values)))
line_chart.add('Employed Variance', list(map(get_employed_variance, values)))
line_chart.add('UnEmployed Mean', list(map(get_unemployed_mean, values)))
line_chart.add('UnEmployed Variance', list(map(get_unemployed_variance, values)))

line_chart.render_to_png('graph.png')