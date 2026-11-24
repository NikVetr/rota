data {
  int<lower=2> T;
  vector[T] y;
}
parameters {
  real mu;
  real<lower=0> sigma;
  real<lower=0, upper=1> phi;
  vector[T] h;
}
model {
  mu ~ normal(0, 1);
  sigma ~ normal(0, 0.5);
  phi ~ beta(20, 1.5);

  h[1] ~ normal(mu, sigma / sqrt(1 - phi^2));
  for (t in 2:T) {
    h[t] ~ normal(mu + phi * (h[t-1] - mu), sigma);
  }

  y ~ normal(0, exp(0.5 * h));
}
generated quantities {
  vector[T] y_rep;
  for (t in 1:T) {
    y_rep[t] = normal_rng(0, exp(0.5 * h[t]));
  }
}
