
#include <iostream>
#include <math.h>

#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::depends(RcppParallel)]]
#include <RcppParallel.h>
using namespace RcppParallel;


double to_radians_cpp(double degrees){
  return(degrees * 3.141593 / 180);
}

double haversine_cpp(double lat1, double long1,
                     double lat2, double long2,
                     std::string unit="km"){
  int radius = 6378;
  double delta_phi = to_radians_cpp(lat2 - lat1);
  double delta_lambda = to_radians_cpp(long2 - long1);
  double phi1 = to_radians_cpp(lat1);
  double phi2 = to_radians_cpp(lat2);
  double term1 = pow(sin(delta_phi / 2), 2);
  double term2 = cos(phi1) * cos(phi2) * pow(sin(delta_lambda/2), 2);
  double the_terms = term1 + term2;
  double delta_sigma = 2 * atan2(sqrt(the_terms), sqrt(1-the_terms));
  double distance = radius * delta_sigma;
  
  /* if it is anything *but* km it is miles */
  if(unit != "km"){
    return(distance*0.621371);
  }
  
  return(distance);
}

// [[Rcpp::export]]
double all_cpp(Rcpp::NumericMatrix& mat) {
  int nrow = mat.nrow();
  int numcomps = nrow*(nrow-1)/2;
  double running_sum = 0;
  for( int i = 0; i < nrow; i++ ){
    for( int j = i+1; j < nrow; j++){
      running_sum += haversine_cpp(mat(i,0), mat(i,1),
                                   mat(j,0), mat(j,1));
    }
  }
  return running_sum / numcomps;
}


struct RunningSum : public Worker
{   
  // source vector
  const RMatrix<double> mat;
  
  // accumulated value
  double sum;
  
  // constructors
  RunningSum(const NumericMatrix input) : mat(input), sum(0) {}
  RunningSum(const RunningSum& sum, Split) : mat(sum.mat), sum(0) {}
  
  void operator()(std::size_t begin, std::size_t end) {
    for( int i = begin; i < end; i++ ){
      for( int j = i+1; j < mat.nrow(); j++){
        sum += haversine_cpp(mat(i,0), mat(i,1),
                             mat(j,0), mat(j,1));
      }
    }
  }
  
  // join my value with that of another Sum
  void join(const RunningSum& rhs) { 
    sum += rhs.sum; 
  }
};

// [[Rcpp::export]]
double all_cpp_parallel(Rcpp::NumericMatrix& mat) {
  
  int nrow = mat.nrow();
  int numcomps = nrow*(nrow-1)/2;
  
  RunningSum worker(mat);
  parallelReduce(0, nrow, worker);

  return worker.sum / numcomps;
}


/***R

air.locs <- read.csv("airportcodes.csv", stringsAsFactors=FALSE)

stopifnot(all.equal(all_cpp(as.matrix(air.locs[,-1])),
                    all_cpp_parallel(as.matrix(air.locs[,-1]))))

library(rbenchmark)
res <- benchmark(all_cpp(as.matrix(air.locs[,-1])),
                 all_cpp_parallel(as.matrix(air.locs[,-1])),
                 columns = c("test", "replications", "elapsed", "relative"),
                 order="relative", replications=10)
res
*/



