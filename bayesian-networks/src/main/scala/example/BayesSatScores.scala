package example

import dk.bayes.dsl.infer
import dk.bayes.dsl.variable.Categorical

object BayesSatScores {

  def run() = {

    val difficulty = Categorical(Vector(0.6, 0.4))
    val intelli = Categorical(Vector(0.7, 0.3))
    val grade = Categorical(
      intelli,
      difficulty,
      Vector(0.3, 0.4, 0.3, 0.05, 0.25, 0.7, 0.9, 0.08, 0.02, 0.5, 0.3, 0.2)
    )
    val sat = Categorical(intelli, Vector(0.95, 0.05, 0.2, 0.8))
    val letter = Categorical(grade, Vector(0.1, 0.9, 0.4, 0.6, 0.99, 0.01))

    println(infer(difficulty).cpd)

  }

}
