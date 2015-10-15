//------------------------------------------------------------------------------
//! @file PrefetchOracle.hh
//! @author Paul Hermann Lensing
//! @brief Predict future of a sequence based on history
//------------------------------------------------------------------------------
#ifndef KINETICIO_PREFETCHORACLE_HH
#define	KINETICIO_PREFETCHORACLE_HH

/*----------------------------------------------------------------------------*/
#include <deque>
#include <list>
/*----------------------------------------------------------------------------*/


namespace kio{

//------------------------------------------------------------------------------
//! Predict future of a sequence based on history 
//------------------------------------------------------------------------------
class PrefetchOracle{
public:
  //----------------------------------------------------------------------------
  //! Do a complete prediction or only partial, non-overlapping with past
  //! prediction requests.
  //----------------------------------------------------------------------------
  enum class PredictionType{ COMPLETE, CONTINUE };

  //----------------------------------------------------------------------------
  //! Add number to the front of the existing sequence
  //!
  //! @param number number is added to existing sequence
  //----------------------------------------------------------------------------
  void add(int number);

  //----------------------------------------------------------------------------
  //! See if sequence has an obvious pattern, predict up to capacity steps 
  //! in the future. 
  //!
  //! @param type if type is CONTINUE, only values that have not been returned
  //!   by previous prediction requests will be considered.
  //! @return a list of predicted future requests. Can be empty if no prediction 
  //!   could be made.
  //----------------------------------------------------------------------------
  std::list<int> predict(PredictionType type = PredictionType::COMPLETE);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_prediction the maximum elements to be predicted
  //----------------------------------------------------------------------------
  PrefetchOracle(std::size_t max_prediction = 10);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~PrefetchOracle();

private:
  //! maximum size of sequence
  std::size_t sequence_capacity;
  //! sequence to base predictions on
  std::deque<int> sequence;
  //! numbers returned in past prediction
  std::deque<int> past_prediction;
};

}

#endif