package main

import (
	controllers "spark-api/controllers"

	"github.com/gin-gonic/gin"
)

//
func main() {
	router := gin.Default()

	v1 := router.Group("/api/v1")
	{
		v1.GET("/sparkapp/:app", controllers.SparkAppControllerHandler())
		v1.GET("/sparkapp/", controllers.SparkAppControllerHandler())
	}

	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"msg": "Not found"})
	})

	router.Run(":8087")

}
