//------------------------------------------------------------------------------
//! @file PrefetchOracle.hh
//! @author Paul Hermann Lensing
//! @brief Predict future of a sequence based on history
//------------------------------------------------------------------------------

/************************************************************************
 * KineticIo - a file io interface library to kinetic devices.          *
 *                                                                      *
 * This Source Code Form is subject to the terms of the Mozilla         *
 * Public License, v. 2.0. If a copy of the MPL was not                 *
 * distributed with this file, You can obtain one at                    *
 * https://mozilla.org/MP:/2.0/.                                        *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but is provided AS-IS, WITHOUT ANY WARRANTY; including without       *
 * the implied warranty of MERCHANTABILITY, NON-INFRINGEMENT or         *
 * FITNESS FOR A PARTICULAR PURPOSE. See the Mozilla Public             *
 * License for more details.                                            *
 ************************************************************************/

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
  //! @param length the prediction length requested, cannot be larger than 
  //!   max_prediction
  //! @param type if type is CONTINUE, only values that have not been returned
  //!   by previous prediction requests will be returned.
  //! @return a list of predicted future requests. Can be empty if no prediction 
  //!   could be made.
  //----------------------------------------------------------------------------
  std::list<int> predict(std::size_t length, PredictionType type = PredictionType::COMPLETE);

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
  //! maximum size of prediction
  const std::size_t max_prediction;
  //! maximum size of sequence
  const std::size_t sequence_capacity;
  //! sequence to base predictions on
  std::deque<int> sequence;
  //! numbers returned in past prediction
  std::deque<int> past_prediction;
};

}

#endif