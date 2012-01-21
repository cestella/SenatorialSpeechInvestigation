args<-commandArgs(trailingOnly=TRUE)
in_file <- args[2]
out_file <- args[3]
data<-read.csv( in_file )
attach(data)
png(out_file)
kde <- density(Mean)
f<-approxfun(kde$x,kde$y,rule=2)

#compute the liberal right boundary 
#to be the first 1/3rd of the probability space
liberal_boundary <- -2
for(i in seq(-2.99,3.0, by=.01)) {
        integral <- integrate(f, -3, i)$value
        if(integral > 1.0/3) {
                liberal_boundary<- i 
                break()
        }
}

#compute the conservative left boundary
#to be the next 1/3rd of the probability space

conservative_boundary <- -2
for(i in seq(liberal_boundary,3.0, by=.01)) {
        integral <- integrate(f, liberal_boundary, i)$value
        if(integral > 1.0/3) {
                conservative_boundary<- i 
                break()
        }
}

#so the partition is (-3, conservative_boundary, liberal_boundary, 3)

liberal_poly.x <- c(-3, seq(-3, liberal_boundary, 0.1), liberal_boundary)
liberal_poly.y <- c(0, f(seq(-3,liberal_boundary, 0.1)), 0)

conservative_poly.x <- c(conservative_boundary, seq(conservative_boundary, 3, 0.1), 3)
conservative_poly.y <- c(0, f(seq(conservative_boundary, 3, 0.1)), 0)

print(paste("liberal right boundary", liberal_boundary, sep=" -> "))
print(paste("conservative left boundary", conservative_boundary, sep=" -> "))

#plot the density function and color code the left, right and middle
plot( kde 
    , main="Density Plot of Political Spectrum"
    , xlab="Political Spectrum"
    , ylab="Density"
    )
polygon(conservative_poly.x, conservative_poly.y, col='red')
polygon(liberal_poly.x, liberal_poly.y, col='blue')
dev.off()
