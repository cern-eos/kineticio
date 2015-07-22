#include "SequencePatternRecognition.hh"
#include <functional>

using namespace kio;

/* basically std::any_of, but using std algorithms without lambdas (gcc 4.4.7) 
 * is really awkward. */
bool list_contains(const std::list<int> &list, int number)
{
  for (auto it = list.cbegin(); it != list.cend(); it++) {
    if (*it == number)
      return true;
  }
  return false;
}

SequencePatternRecognition::SequencePatternRecognition(std::size_t max)
    : sequence_capacity(max) { }

SequencePatternRecognition::~SequencePatternRecognition() { }

void SequencePatternRecognition::add(int number)
{
  if (!list_contains(sequence, number)) {
    sequence.push_front(number);
    if (sequence.size() > sequence_capacity)
      sequence.pop_back();
  }
}

std::list<int> SequencePatternRecognition::predict(PredictionType type)
{
  std::list<int> prediction;
  if (sequence.empty())
    return std::move(prediction);

  /* If only a single value has been requested so far, be aggressive and
     assume sequentiality. */
  if (sequence.size() == 1)
    prediction.push_back(sequence.front() + 1);
  else {
    /* get diff vector */
    std::list<int> diff;
    for (auto it = ++sequence.cbegin(); it != sequence.cend(); it++) {
      diff.push_back(*std::prev(it) - *it);
    }

    /* immediate neighbor pattern detection */
    auto length = 0;
    for (auto it = ++diff.cbegin(); it != diff.cend(); it++) {
      if (*std::prev(it) == *it)
        length++;
      else
        break;
    }
    /* TODO: detect irregular patterns, e.g. 1,2,4,5,7,8 */

    /* build prediction list */
    for (int i = 1; i <= length + 1; i++)
      prediction.push_back(sequence.front() + i * diff.front());
  }
  /* if type == continue, don't predict things that already have been predicted */
  if (type == PredictionType::CONTINUE) {
    prediction.remove_if(
        std::bind(list_contains, std::cref(past_prediction), std::placeholders::_1)
    );
  }

  /* keep past prediction list up to date */
  for (auto it = prediction.cbegin(); it != prediction.cend(); it++)
    past_prediction.push_front(*it);
  if (past_prediction.size() > sequence_capacity)
    past_prediction.resize(sequence_capacity);

  return std::move(prediction);
}