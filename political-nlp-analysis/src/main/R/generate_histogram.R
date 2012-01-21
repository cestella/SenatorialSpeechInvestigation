args<-commandArgs(trailingOnly=TRUE)
in_file <- args[2]
out_file <- args[3]
data<-read.csv( in_file )
attach(data)
png(out_file)
kde <- density(Mean)
f<-approxfun(kde$x,kde$y,rule=2)

#compute the conservative right boundary 
#to be the first 1/3rd of the probability space
conservative_boundary <- -2
for(i in seq(-3.01,2.0, by=.01)) {
        integral <- integrate(f, -3, i)$value
        if(integral > 1.0/3) {
                conservative_boundary<- i 
                break()
        }
}

#compute the liberal left boundary
#to be the next 1/3rd of the probability space

liberal_boundary <- -2
for(i in seq(conservative_boundary,2.0, by=.01)) {
        integral <- integrate(f, conservative_boundary, i)$value
        if(integral > 1.0/3) {
                liberal_boundary<- i 
                break()
        }
}

#so the partition is (-3, conservative_boundary, liberal_boundary, 3)

conservative_poly.x <- c(-3, seq(-3, conservative_boundary, 0.1), conservative_boundary)
conservative_poly.y <- c(0, f(seq(-3,conservative_boundary, 0.1)), 0)

liberal_poly.x <- c(liberal_boundary, seq(liberal_boundary, 3, 0.1), 3)
liberal_poly.y <- c(0, f(seq(liberal_boundary, 3, 0.1)), 0)

print(paste("conservative right boundary", conservative_boundary, sep=" -> "))
print(paste("liberal left boundary", liberal_boundary, sep=" -> "))

#plot the density function and color code the left, right and middle
plot( kde 
    , main="Density Plot of Political Spectrum"
    , xlab="Political Spectrum"
    , ylab="Density"
    )
polygon(conservative_poly.x, conservative_poly.y, col='red')
polygon(liberal_poly.x, liberal_poly.y, col='blue')
dev.off()
