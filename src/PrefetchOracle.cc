#include "PrefetchOracle.hh"
#include <functional>
#include <unordered_map>
#include "Logging.hh"

using namespace kio;

/* basically std::any_of, but using std algorithms without lambdas (gcc 4.4.7) 
 * is really awkward. */
static bool _contains(const std::deque<int> &container, int number)
{
  for (auto it = container.cbegin(); it != container.cend(); it++) {
    if (*it == number)
      return true;
  }
  return false;
}

PrefetchOracle::PrefetchOracle(std::size_t max)
    : max_prediction(max), sequence_capacity(max_prediction > 8 ? max_prediction+2 : 10)
{
}

PrefetchOracle::~PrefetchOracle() { }

void PrefetchOracle::add(int number)
{  
  if (!_contains(sequence, number)) {
    sequence.push_front(number);
    if (sequence.size() > sequence_capacity)
      sequence.pop_back();
  }
}

std::list<int> PrefetchOracle::predict(size_t length, PredictionType type)
{
  if(length > max_prediction){
    kio_warning("Requested prediction length ", length, " is larger than maximum prediction size of ", max_prediction);
    length = max_prediction;
  }
  
  std::list<int> prediction;
  
  /* Can't make predictions without history. */
  if(sequence.size() < 3)
    return prediction; 
  
  /* get distances between sequence elements. to take outliers into account, we take the immediate distance
   * and the one-past immediate distance. */
  std::unordered_map<int, std::deque<int>> distances;
  for (auto it = sequence.cbegin(); it != sequence.cend(); it++) {
    auto p1 = std::next(it);
    if(p1 != sequence.cend()){
      distances[*it-*p1].push_back(*it); 
      auto p2 = std::next(p1); 
      if(p2 != sequence.cend()){
        distances[*it-*p2].push_back(*it); 
      }
    }
  }
  
  /* find the most frequent distance */
  auto distance = distances.cbegin();
  for(auto it = distances.cbegin(); it != distances.cend(); it++){
    if(it->second.size() > distance->second.size())
      distance = it; 
  }
  
  /* We are only confident enough to make a prediction if we have a certain distance frequency s*/ 
  if(distance->second.size() < static_cast<int>(sequence.size() * 0.75))
    return prediction;  
  
  /* build prediction list */
  for (int i = 1; i < distance->second.size() + 1; i++){
    int p = distance->second.front() + i * distance->first; 
    /* never predict negative block numbers */
    if(p>0 && prediction.size() < length)
      prediction.push_back(p);
  }
  
  /* if type == continue, don't predict things that already have been predicted */
  if (type == PredictionType::CONTINUE) {
    prediction.remove_if(
        std::bind(_contains, std::cref(past_prediction), std::placeholders::_1)
    );
  }

  /* keep past prediction list up to date */
  for (auto it = prediction.cbegin(); it != prediction.cend(); it++)
    past_prediction.push_front(*it);
  if (past_prediction.size() > sequence_capacity)
    past_prediction.resize(sequence_capacity);

  return prediction;
}
