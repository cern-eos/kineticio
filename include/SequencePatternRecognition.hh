//------------------------------------------------------------------------------
//! @file SequencePatternRecognition.hh
//! @author Paul Hermann Lensing
//! @brief Predict future of a sequence based on recognizing simple patterns
//------------------------------------------------------------------------------
#ifndef SEQUENCEPATTERNRECOGNITION_HH
#define	SEQUENCEPATTERNRECOGNITION_HH

/*----------------------------------------------------------------------------*/
#include <list>
/*----------------------------------------------------------------------------*/


namespace kio{

//------------------------------------------------------------------------------
//! Predict future of a sequence based on recognizing simple patterns
//------------------------------------------------------------------------------
class SequencePatternRecognition{
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
  //! See if sequence has an obvious pattern, predict up to capacity-1
  //! steps into the future.
  //!
  //! @param type if type is CONTINUE, only values that have not been returned
  //!   by previous prediction requests will be considered.
  //! @return a list of predicted future requested, at most capcity-1 length.
  //!   Can be empty if no prediction could be made.
  //----------------------------------------------------------------------------
  std::list<int> predict(PredictionType type = PredictionType::COMPLETE);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param sequence_capacity the maximum size of the stored sequence to base
  //!   predictions on.
  //----------------------------------------------------------------------------
  SequencePatternRecognition(std::size_t sequence_capacity = 10);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SequencePatternRecognition();

private:
  //! maximum size of sequence
  std::size_t sequence_capacity;
  //! sequence to base predictions on
  std::list<int> sequence;
  //! numbers returned in past prediction
  std::list<int> past_prediction;
};

}

#endif	/* SEQUENCEPATTERNRECOGNITION_HH */

